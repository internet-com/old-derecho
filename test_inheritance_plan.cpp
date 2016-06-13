#include <SerializationSupport.hpp>
#include <queue>
#include <future>
#include <FunctionalMap.hpp>

namespace rpc{

	template<typename t>
	auto& operator<<(std::ostream& out, const std::vector<t>& v){
		out << "{";
		for (const auto &e : v){
			out << e << ", ";
		}
		out << "}";
		return out;
	}

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
	using who_t = std::vector<Opcode>;
	using Node_id = typename who_t::value_type;

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
								const Node_id &,
								const char * recv_buf,
								const std::function<char* (int)>& out_alloc)>;

	template<typename T>
	using reply_map = std::map<Node_id,std::future<T> >;

	template<typename T>
	struct PendingResults{
		std::promise<std::unique_ptr<reply_map<T> > > pending_map;
		std::map<Node_id,std::promise<T> > populated_promises;
		
		void fulfill_map(const who_t &who){
			std::unique_ptr<reply_map<T> > to_add{new reply_map<T>{}};
			for (const auto &e : who){
				to_add->emplace(e, populated_promises[e].get_future());
			}
			pending_map.set_value(std::move(to_add));
		}

		std::promise<T>& receive_message(const Node_id& nid){
			return populated_promises.at(nid);
		}
	};

	template<typename T>
	struct QueryResults{
		using map_fut = std::future<std::unique_ptr<reply_map<T> > >;
		using map = reply_map<T>;

		map_fut pending_rmap;
		map rmap;

		bool valid(const Node_id &nid){
			return (rmap.size() > 0) && rmap.at(nid).valid();
		}

		auto get(const Node_id &nid){
			if (rmap.size() == 0){
				assert(pending_rmap.valid());
				rmap = std::move(pending_rmap.get());
			}
			assert(rmap.at(nid).valid());
			return rmap.at(nid).get();
		}
	};
	
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
		
		std::queue<std::map<Node_id, std::promise<Ret> > > ret;
		std::mutex ret_lock;
		using lock_t = std::unique_lock<std::mutex>;

		//use this from within a derived class to receive precisely this RemoteInvocable
		//(this way, all RemoteInvocable methods do not need to worry about type collisions)
		inline RemoteInvocable& handler(std::integral_constant<Opcode, tag> const * const, const Args & ...) {
			return *this;
		}

		using barray = char*;
		using cbarray = const char*;
		template<typename A>
		std::size_t to_bytes(barray& v, const A & a){
			std::cout << "serializing::" << (void*) v << "::" << a << std::endl;
			auto size = mutils::to_bytes(a, v);
			v += size;
			return size;
		}
		
		std::tuple<int, char *, std::map<Node_id, std::future<Ret> > > Send(const who_t& destinations,const std::function<char *(int)> &out_alloc,
														const std::decay_t<Args> & ...a){
			const auto size = (mutils::bytes_size(a) + ... + 0);
			const auto serialized_args = out_alloc(size);
			{
				auto v = serialized_args;
				auto check_size = (to_bytes(v,a) + ... + 0);
				assert(check_size == size);
			}
			
			lock_t l{ret_lock};
			//default-initialize the maps
			ret.emplace();
			auto tuple_ret = std::make_tuple(size,serialized_args,std::map<Node_id, std::future<Ret> >{});
			auto &promise_map = ret.back();
			auto &future_map = std::get<2>(tuple_ret);
			for (auto &d : destinations){
				future_map.emplace(d, promise_map[d].get_future());
			}
			return tuple_ret;
		}
		

		
		inline recv_ret receive_response(mutils::DeserializationManager* dsm, const Node_id &who, const char* response, const std::function<char*(int)>&){
			lock_t l{ret_lock};
			//got an unexpected reply, probably because we're done receiving for this function
			if (!ret.front().count(who)) ret.pop();
			ret.front().at(who).set_value(*mutils::from_bytes<Ret>(dsm,response));
			return recv_ret{0,0,nullptr};
		}

		
		template<typename _Type>
		inline auto deserialize(mutils::DeserializationManager *dsm, cbarray& mut_in){
			using Type = std::decay_t<_Type>;
			auto ds = mutils::from_bytes<Type>(dsm,mut_in);
			const auto size = mutils::bytes_size(*ds);
			mut_in += size;
			std::cout << "deserializing::" << (void*)mut_in << "::" << *ds << std::endl;
			return ds;
		}
		

		inline recv_ret receive_call(std::false_type const * const, mutils::DeserializationManager* dsm,
									 const Node_id &, 
							  const char * recv_buf,
							  const std::function<char* (int)>& out_alloc){
			const auto result = f(*deserialize<Args>(dsm,recv_buf)... );
			const auto result_size = mutils::bytes_size(result);
			auto out = out_alloc(result_size);
			mutils::to_bytes(result,out);
			return recv_ret{reply_id,mutils::bytes_size(result),out};
		}
		
		inline recv_ret receive_call(std::true_type const * const, mutils::DeserializationManager* dsm,
									 const Node_id &, 
							  const char * recv_buf,
							  const std::function<char* (int)>&){
			f(*deserialize<Args>(dsm,recv_buf)... );
			return recv_ret{reply_id,0,nullptr};
		}

		inline recv_ret receive_call(mutils::DeserializationManager* dsm,
									 const Node_id &who, 
							  const char * recv_buf,
							  const std::function<char* (int)>& out_alloc){
			constexpr std::is_same<Ret,void> *choice{nullptr};
			return receive_call(choice, dsm, who,recv_buf, out_alloc);
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

		inline static auto header_space(const who_t &who) {
			return sizeof(Opcode) + mutils::bytes_size(who);
		}
		
		inline static char* extra_alloc (const who_t &who, int i){
			const auto hs = header_space(who);
			return (char*) calloc(i + hs,sizeof(char)) + hs;
		}
		
	public:
		const who_t empty;
		
		void receive_call_loop(){
			using namespace std::placeholders;
			while (alive){
				auto recv_pair = lm.receive();
				Node_id received_from{0};
				auto *buf = recv_pair.second;
				auto size = recv_pair.first;
				assert(size);
				auto indx = ((Opcode*)buf)[0];
				assert(indx);
				auto who = mutils::from_bytes<who_t>(&dsm,buf + sizeof(Opcode));
				buf += header_space(*who);
				auto reply_tuple = receivers->at(indx)(&dsm, received_from,buf, std::bind(extra_alloc,empty,_1));
				auto * reply_buf = std::get<2>(reply_tuple);
				if (reply_buf){
					//we don't need to tell the destination
					//any information about where they should
					//send replies, because the destination
					//isn't sending any replies.
					reply_buf -= header_space(empty);
					const auto id = std::get<0>(reply_tuple);
					const auto size = std::get<1>(reply_tuple);
					((Opcode*)reply_buf)[0] = id;
					lm.send(size + header_space(empty),reply_buf);
					free(reply_buf);
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
		auto Send(const who_t &who, Args && ... args){
			//this "who" is the destination of this send.
			//the "who" we include in this payload should
			//be all targets which expect a reply.
			who_t send_replies;
			send_replies.emplace_back(0);
			using namespace std::placeholders;
			constexpr std::integral_constant<Opcode, tag>* choice{nullptr};
			auto &hndl = this->handler(choice,args...);
			auto sent_tuple = hndl.Send(who,std::bind(extra_alloc,send_replies,_1),
										std::forward<Args>(args)...);
			std::size_t used = std::get<0>(sent_tuple);
			char * buf = std::get<1>(sent_tuple) - header_space(send_replies);
			((Opcode*)buf)[0] = hndl.invoke_id;
			mutils::to_bytes(send_replies,buf + sizeof(Opcode));
			lm.send(used + sizeof(Opcode),buf);
			free(buf);
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

auto test2(int i, double d, Opcode c){
	std::cout << "called with " << i << "::" << d << "::" << (int)c  << std::endl;
	return i + d + c;
}

auto test3(const std::vector<Opcode> & oc){
	return oc.front();
}

int main() {
	auto msg_pair = LocalMessager::build_pair();
	HANDLERS[SET_1] += something_here;
	auto hndlers1 = handlers(std::move(msg_pair.first),0,test1,0,test2,0,test3);
	/*
	handlers1 = {0,test1} + {0,test2} + {0,test3};
	HANDLERS[0] += test1;
	HANDLERS[0] += test2;
	HANDLERS[0] += test3;*/
	auto hndlers2 = handlers(std::move(msg_pair.second),0,test1,0,test2,0,test3);
	who_t other;
	other.emplace_back(0);

	/*
	OrderedQuery<Opcode>(ALL,arguments...) --> future<map<Node_id,std::future<Return> > >; //make a type alias that wraps this

	P2PQuery<Opcode>(who,arguments...) --> std::future<Return>; //simple case

	P2PSend<Opcode>(who,arguments...) --> void; //simple case

	OrderedSend<Opcode>(ALL,arguments...) --> void;
	OrderedSend<Opcode>(arguments...) --> void; //assume "ALL"

	auto &returned_map = hndlers1->Send<0>(other,1);

	for (auto &pair : returned_map){
		if (pair.second.valid())
			break; //found it!
	}
	*/	

	assert(hndlers1->Send<0>(other,1).at(0).get() == 1);
	assert(hndlers1->Send<0>(other,1,2,3).at(0).get() == 6);
	assert((hndlers1->Send<0,const std::vector<Opcode> &>(other,{1,2,3}).at(0).get() == 1));
	std::cout << "done" << std::endl;
}
