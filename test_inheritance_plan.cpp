#include <SerializationSupport.hpp>
#include <queue>
#include <future>

namespace rpc{

	/*
	template<typename T>
	auto& print_all(T& t){
		return t;
	}
	
	template<typename T, typename A, typename... B>
	auto& print_all(T& t, const A &a, const B&... b){
		return print_all(t << a,b...);
	}//*/

	using Opcode = unsigned long long;

	template<Opcode, typename>
	struct RemoteInvocable;
	
	class LocalMessager{
	private:
		LocalMessager(){}
		using elem = std::pair<std::size_t, char const * const>;
		using queue_t = std::queue<elem>;
		
		std::shared_ptr<queue_t> _send;
		std::shared_ptr<queue_t> _receive;
		using l = std::unique_lock<std::mutex>;
		std::shared_ptr<std::mutex> m_send;
		std::shared_ptr<std::condition_variable> cv_send;
		std::shared_ptr<std::mutex> m_receive;
		std::shared_ptr<std::condition_variable> cv_receive;
		LocalMessager(decltype(_send) &_send,
					  decltype(_receive) &_receive,
					  decltype(m_send) &m_send,
					  decltype(cv_send) &cv_send,
					  decltype(m_receive) &m_receive,
					  decltype(cv_receive) &cv_receive)
			:_send(_send),
			 _receive(_receive),
			 m_send(m_send),
			 cv_send(cv_send),
			 m_receive(m_receive),
			 cv_receive(cv_receive){}
	public:
		LocalMessager(const LocalMessager&) = delete;
		LocalMessager(LocalMessager&&) = default;
		
		static std::pair<LocalMessager,LocalMessager> build_pair(){
			std::shared_ptr<queue_t> q1{new queue_t{}};
			std::shared_ptr<queue_t> q2{new queue_t{}};
			std::shared_ptr<std::mutex> m1{new std::mutex{}};
			std::shared_ptr<std::condition_variable> cv1{new std::condition_variable{}};
			std::shared_ptr<std::mutex> m2{new std::mutex{}};
			std::shared_ptr<std::condition_variable> cv2{new std::condition_variable{}};
			return std::pair<LocalMessager,LocalMessager>{LocalMessager{q1,q2,m1,cv1,m2,cv2},LocalMessager{q2,q1,m2,cv2,m1,cv1}};
		}
		
		void send(std::size_t s, char const * const v){
			l e{*m_send};
			cv_send->notify_all();
			assert(s);
			_send->emplace(s,v);
		}
		
		elem receive(){
			l e{*m_receive};
			while (_receive->empty()){
				cv_receive->wait(e);
				}
			auto ret = _receive->front();
			_receive->pop();
			return ret;
		}
	};
	
	using recv_ret = std::tuple<Opcode,std::size_t, char *>;
	
	using receive_fun_t =
		std::function<recv_ret (mutils::DeserializationManager* dsm,
								const char * recv_buf,
								const std::function<char* (int)>& out_alloc)>;
	
	
//many versions of this class will be extended by a single Hanlders context.
//each specific instance of this class provies a mechanism for communicating with
//remote sites.
	template<Opcode tag, typename Ret, typename... Args>
	struct RemoteInvocable<tag, Ret (Args...)> {
		
		using f_t = Ret (*) (Args...);
		const f_t f;
		static const Opcode invoke_id;
		static const Opcode reply_id;
		
		RemoteInvocable(std::map<Opcode,receive_fun_t> &receivers, Ret (*f) (Args...)):f(f){
			receivers[invoke_id] = [this](auto... a){return receive_call(a...);};
			receivers[reply_id] = [this](auto... a){return receive_response(a...);};
		}
		
		std::queue<std::promise<Ret> > ret;
		std::mutex ret_lock;
		using lock_t = std::unique_lock<std::mutex>;

		//use this from within a derived class to receive precisely this RemoteInvocable
		//(this way, all RemoteInvocable methods do not need to worry about type collisions)
		inline RemoteInvocable& handler(std::integral_constant<Opcode, tag> const * const, const Args & ...) {
			return *this;
		}

		using barray = char*;
		template<typename A>
		std::size_t to_bytes(barray& v, A && a){
			auto size = mutils::to_bytes(std::forward<A>(a), v);
			v += size;
			return size;
		}
		
		std::tuple<int, char *, std::future<Ret> > Send(const std::function<char *(int)> &out_alloc,
														  Args && ...a){
			const auto size = (mutils::bytes_size(a) + ... + 0);
			const auto serialized_args = out_alloc(size);
			{
				auto v = serialized_args;
				auto check_size = (to_bytes(v,std::forward<Args>(a)) + ... + 0);
				assert(check_size == size);
			}
			
			lock_t l{ret_lock};
			ret.emplace();
			return std::make_tuple(size,serialized_args,ret.back().get_future());
		}
		

		
		inline recv_ret receive_response(mutils::DeserializationManager* dsm, const char* response, const std::function<char*(int)>&){
			lock_t l{ret_lock};
			ret.front().set_value(*mutils::from_bytes<Ret>(dsm,response));
			ret.pop();
			return recv_ret{0,0,nullptr};
		}

		
		template<typename _Type>
		inline auto deserialize(mutils::DeserializationManager *dsm, const char * mut_in){
			using Type = std::decay_t<_Type>;
			auto ds = mutils::from_bytes<Type>(dsm,mut_in);
			const auto size = mutils::bytes_size(*ds);
			mut_in += size;
			return ds;
		}
		

		inline recv_ret receive_call(std::false_type const * const, mutils::DeserializationManager* dsm,
							  const char * recv_buf,
							  const std::function<char* (int)>& out_alloc){
			const auto result = f(*deserialize<Args>(dsm,recv_buf)... );
			const auto result_size = mutils::bytes_size(result);
			auto out = out_alloc(result_size);
			mutils::to_bytes(result,out);
			return recv_ret{reply_id,mutils::bytes_size(result),out};
		}
		
		inline recv_ret receive_call(std::true_type const * const, mutils::DeserializationManager* dsm,
							  const char * recv_buf,
							  const std::function<char* (int)>&){
			f(*deserialize<Args>(dsm,recv_buf)... );
			return recv_ret{reply_id,0,nullptr};
		}

		inline recv_ret receive_call(mutils::DeserializationManager* dsm,
							  const char * recv_buf,
							  const std::function<char* (int)>& out_alloc){
			constexpr std::is_same<Ret,void> *choice{nullptr};
			return receive_call(choice, dsm, recv_buf, out_alloc);
		}
	};
	
	template<Opcode tag, typename Ret, typename... Args>
	const Opcode RemoteInvocable<tag, Ret (Args...)>::invoke_id{mutils::gensym()};

	template<Opcode tag, typename Ret, typename... Args>
	const Opcode RemoteInvocable<tag, Ret (Args...)>::reply_id{mutils::gensym()};
	
	template<typename...>
	struct RemoteInvocablePairs;
	
	template<Opcode id, typename Q>
	struct RemoteInvocablePairs<std::integral_constant<Opcode, id>,Q>
		: public RemoteInvocable<id,Q> {
		RemoteInvocablePairs(std::map<Opcode,receive_fun_t> &receivers, Q q)
			:RemoteInvocable<id,Q>(receivers, q){}
		
		using RemoteInvocable<id,Q>::handler;
	};
	
//id better be an integral constant of Opcode
	template<Opcode id, typename Q,typename... rest>
	struct RemoteInvocablePairs<std::integral_constant<Opcode, id>, Q, rest...> :
		public RemoteInvocable<id,Q>,
		public RemoteInvocablePairs<rest...> {

	public:
		
		template<typename... T>
		RemoteInvocablePairs(std::map<Opcode,receive_fun_t> &receivers, Q q, T && ... t)
			:RemoteInvocable<id,Q>(receivers, q),
			RemoteInvocablePairs<rest...>(receivers,std::forward<T>(t)...){}

		using RemoteInvocable<id,Q>::handler;
		using RemoteInvocablePairs<rest...>::handler;
	};
	
	template<typename... Fs>
	struct Handlers : private RemoteInvocablePairs<Fs...> {
	private:
		//point-to-point communication
		LocalMessager lm;
		bool alive{true};
		//constructed *before* initialization
		std::unique_ptr<std::map<Opcode, receive_fun_t> > receivers;
		//constructed *after* initialization
		std::unique_ptr<std::thread> receiver;
		mutils::DeserializationManager dsm{{}};
		
		static char* extra_alloc (int i){
			return (char*) calloc(i + sizeof(Opcode),sizeof(char)) + sizeof(Opcode);
		}
		
	public:
		
		void receive_call_loop(){
			while (alive){
				auto recv_pair = lm.receive();
				auto *buf = recv_pair.second;
				auto size = recv_pair.first;
				assert(size);
				assert(size >= sizeof(Opcode));
				auto indx = ((Opcode*)buf)[0];
				assert(indx);
				auto reply_tuple = receivers->at(indx)(&dsm,buf + sizeof(Opcode), extra_alloc);
				auto * reply_buf = std::get<2>(reply_tuple);
				if (reply_buf){
					reply_buf -= sizeof(Opcode);
					const auto id = std::get<0>(reply_tuple);
					const auto size = std::get<1>(reply_tuple);
					((Opcode*)reply_buf)[0] = id;
					lm.send(size + sizeof(Opcode),reply_buf);
				}
			}
		}

		//these are the functions (no names) from Fs
		template<typename... _Fs>
		Handlers(decltype(receivers) rvrs, LocalMessager _lm, _Fs... fs)
			:RemoteInvocablePairs<Fs...>(*rvrs,fs...),
			lm(std::move(_lm)),
			receivers(std::move(rvrs))
			{
				receiver.reset(new std::thread{[&](){receive_call_loop();}});
			}
		
		//these are the functions (no names) from Fs
		//delegation so receivers exists during superclass construction
		template<typename... _Fs>
		Handlers(LocalMessager _lm, _Fs... fs)
			:Handlers(std::make_unique<typename decltype(receivers)::element_type>(), std::move(_lm),fs...){}
		
		~Handlers(){
			alive = false;
			receiver->join();
		}
		
		template<Opcode tag, typename... Args>
			auto Send(Args && ... args){
			constexpr std::integral_constant<Opcode, tag>* choice{nullptr};
			auto &hndl = this->handler(choice,args...);
			auto sent_tuple = hndl.Send(extra_alloc,
										std::forward<Args>(args)...);
			std::size_t used = std::get<0>(sent_tuple);
			char * buf = std::get<1>(sent_tuple) - sizeof(Opcode);
			((Opcode*)buf)[0] = hndl.invoke_id;
			lm.send(used + sizeof(Opcode),buf);
			return std::move(std::get<2>(sent_tuple));
		}

		/*
		template <typename... Args>
		void OrderedSend(Opcode opcode, const Args &... arg);

	template <typename... Args>
    void PaxosSend(Opcode opcode, const Args &... arg);

	template <typename... Args>
    void P2PSend(NodeId who, Opcode opcode, const Args &... arg);

	template <typename Q, typename... Args>
    const QueryReplies<Q>& OrderedQuery(Opcode opcode, const Args &... arg);

	template <typename Q, typename... Args>
    const QueryReplies<Q>& PaxosQuery(Opcode opcode, const Args &... arg);

	template <typename Q, typename... Args>
    const QueryReplies<Q>& P2PQuery(NodeId who, Opcode opcode, const Args &... arg);
		*/
		
	};

	struct Handlers_erased{
		std::shared_ptr<void> erased_handlers;

		template<typename... T>
		Handlers_erased(std::unique_ptr<Handlers<T...> > h):erased_handlers(h.release()){}

		template<Opcode tag, typename Ret, typename... Args>
		auto Send (Args && ... args){
			return static_cast<RemoteInvocable<tag, Ret (Args...) >* >(erased_handlers.get())->Send(std::forward<Args>(args)...);
		}
	};
}

using namespace rpc;

//handles up to 5 args
#define HANDLERS_TYPE_ARGS2(a, b) std::integral_constant<rpc::Opcode, a>, decltype(b)
#define HANDLERS_TYPE_ARGS4(a, b,c...) std::integral_constant<rpc::Opcode, a>, decltype(b), HANDLERS_TYPE_ARGS2(c)
#define HANDLERS_TYPE_ARGS6(a, b,c...) std::integral_constant<rpc::Opcode, a>, decltype(b), HANDLERS_TYPE_ARGS4(c)
#define HANDLERS_TYPE_ARGS8(a, b,c...) std::integral_constant<rpc::Opcode, a>, decltype(b), HANDLERS_TYPE_ARGS6(c)
#define HANDLERS_TYPE_ARGS10(a, b,c...) std::integral_constant<rpc::Opcode, a>, decltype(b), HANDLERS_TYPE_ARGS8(c)
#define HANDLERS_TYPE_ARGS_IMPL2(count, ...) HANDLERS_TYPE_ARGS ## count (__VA_ARGS__)
#define HANDLERS_TYPE_ARGS_IMPL(count, ...) HANDLERS_TYPE_ARGS_IMPL2(count, __VA_ARGS__)
#define HANDLERS_TYPE_ARGS(...) HANDLERS_TYPE_ARGS_IMPL(VA_NARGS(__VA_ARGS__), __VA_ARGS__)

//handles up to 5 args
#define HANDLERS_ONLY_FUNS2(a, b) b
#define HANDLERS_ONLY_FUNS4(a, b,c...) b, HANDLERS_ONLY_FUNS2(c)
#define HANDLERS_ONLY_FUNS6(a, b,c...) b, HANDLERS_ONLY_FUNS4(c)
#define HANDLERS_ONLY_FUNS8(a, b,c...) b, HANDLERS_ONLY_FUNS6(c)
#define HANDLERS_ONLY_FUNS10(a, b,c...) b, HANDLERS_ONLY_FUNS8(c)
#define HANDLERS_ONLY_FUNS_IMPL2(count, ...) HANDLERS_ONLY_FUNS ## count (__VA_ARGS__)
#define HANDLERS_ONLY_FUNS_IMPL(count, ...) HANDLERS_ONLY_FUNS_IMPL2(count, __VA_ARGS__)
#define HANDLERS_ONLY_FUNS(...) HANDLERS_ONLY_FUNS_IMPL(VA_NARGS(__VA_ARGS__), __VA_ARGS__)

#define handlers(m,a...) std::make_unique<Handlers<HANDLERS_TYPE_ARGS(a)> >(m,HANDLERS_ONLY_FUNS(a))

int test1(int i){return i;}

auto test2(int i, double d, char c){return i + d + c;}

int main() {
	auto msg_pair = LocalMessager::build_pair();
	auto hndlers1 = handlers(std::move(msg_pair.first),0,test1,0,test2);
	auto hndlers2 = handlers(std::move(msg_pair.second),0,test1,0,test2);
	assert(hndlers1->Send<0>(1).get() == 1);
	assert(hndlers1->Send<0>(1,2,3).get() == 6);
	std::cout << "done" << std::endl;
}
