#pragma once
#include <SerializationSupport.hpp>
#include <queue>
#include <chrono>
#include <numeric>
#include <future>
#include <FunctionalMap.hpp>

namespace rpc {

template <typename t>
auto &operator<<(std::ostream &out, const std::vector<t> &v) {
    out << "{";
    for(const auto &e : v) {
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

struct Opcode {
    using t = unsigned long long;
    t id;
    Opcode(const decltype(id) &id) : id(id) {}
    Opcode() = default;
    bool operator==(const Opcode &n) const { return id == n.id; }
    bool operator<(const Opcode &n) const { return id < n.id; }
};
auto &operator<<(std::ostream &out, const Opcode &op) { return out << op.id; }
using FunctionTag = unsigned long long;
struct Node_id {
    unsigned long long id;
    Node_id(const decltype(id) &id) : id(id) {}
    Node_id() = default;
    bool operator==(const Node_id &n) const { return id == n.id; }
    bool operator<(const Node_id &n) const { return id < n.id; }
};
auto &operator<<(std::ostream &out, const Node_id &nid) {
    return out << nid.id;
}

using who_t = std::vector<Node_id>;

template <FunctionTag, typename>
struct RemoteInvocable;

class LocalMessager {
private:
    LocalMessager() {}
    using elem = std::pair<std::size_t, char const *const>;
    using queue_t = std::queue<elem>;

    std::shared_ptr<queue_t> _send;
    std::shared_ptr<queue_t> _receive;
    using l = std::unique_lock<std::mutex>;
    std::shared_ptr<std::mutex> m_send;
    std::shared_ptr<std::condition_variable> cv_send;
    std::shared_ptr<std::mutex> m_receive;
    std::shared_ptr<std::condition_variable> cv_receive;
    LocalMessager(decltype(_send) &_send, decltype(_receive) &_receive,
                  decltype(m_send) &m_send, decltype(cv_send) &cv_send,
                  decltype(m_receive) &m_receive,
                  decltype(cv_receive) &cv_receive)
        : _send(_send),
          _receive(_receive),
          m_send(m_send),
          cv_send(cv_send),
          m_receive(m_receive),
          cv_receive(cv_receive) {}

public:
    LocalMessager(const LocalMessager &) = default;
    static std::map<Node_id, LocalMessager> send_to;

    static LocalMessager init_pipe(const Node_id &source);

    static LocalMessager get_send_to(const Node_id &source);

    void send(std::size_t s, char const *const v) {
        assert(((Opcode *)v)[0].id > 0);
        l e{*m_send};
        cv_send->notify_all();
        assert(s);
        _send->emplace(s, v);
    }

    elem receive() {
        l e{*m_receive};
        while(_receive->empty()) {
            cv_receive->wait(e);
        }
        auto ret = _receive->front();
        _receive->pop();
        assert(((const Opcode *)ret.second)[0].id > 0);
        return ret;
    }
};

LocalMessager LocalMessager::get_send_to(const Node_id &source) {
    assert(send_to.count(source));
    return send_to.at(source);
}
LocalMessager LocalMessager::init_pipe(const Node_id &source) {
    std::shared_ptr<queue_t> q1{new queue_t{}};
    std::shared_ptr<queue_t> q2{new queue_t{}};
    std::shared_ptr<std::mutex> m1{new std::mutex{}};
    std::shared_ptr<std::condition_variable> cv1{new std::condition_variable{}};
    std::shared_ptr<std::mutex> m2{new std::mutex{}};
    std::shared_ptr<std::condition_variable> cv2{new std::condition_variable{}};
    send_to.emplace(source, LocalMessager{q1, q2, m1, cv1, m2, cv2});
    return LocalMessager{q2, q1, m2, cv2, m1, cv1};
}
std::map<Node_id, LocalMessager> LocalMessager::send_to;

	struct recv_ret {
		Opcode opcode;
		std::size_t size;
		char *payload;
		std::exception_ptr possible_exception;
	};
	        

using receive_fun_t = std::function<recv_ret(
    mutils::DeserializationManager *dsm, const Node_id &, const char *recv_buf,
    const std::function<char *(int)> &out_alloc)>;

template <typename T>
using reply_map = std::map<Node_id, std::future<T> >;

template <typename T>
struct QueryResults {
    using map_fut = std::future<std::unique_ptr<reply_map<T> > >;
    using map = reply_map<T>;
	using type = T;

    map_fut pending_rmap;
    QueryResults(map_fut pm) : pending_rmap(std::move(pm)) {}
	struct ReplyMap {
	private:
		QueryResults &parent;
	public:
		map rmap;
		
		ReplyMap(QueryResults& qr):parent(qr){};
		ReplyMap(const ReplyMap&) = delete;
		ReplyMap(ReplyMap&& rm)
			:parent(rm.parent),rmap(std::move(rm.rmap)){}
		
		bool valid(const Node_id &nid){
			assert(rmap.size() == 0 || rmap.count(nid));
			return (rmap.size() > 0) && rmap.at(nid).valid();
		}
		
		/*
		  returns true if we sent to this node, 
		  regardless of whether this node has replied.
		*/
		bool contains(const Node_id &nid){
			return rmap.count(nid);
		}
		
		auto begin(){
			return std::begin(rmap);
		}
		
		auto end(){
			return std::end(rmap);
		}
		
		auto get(const Node_id &nid){
			if (rmap.size() == 0){
				assert(parent.pending_rmap.valid());
				rmap = std::move(*parent.pending_rmap.get());
			}
			assert(rmap.size() > 0);
			assert(rmap.count(nid));
			assert(rmap.at(nid).valid());
			return rmap.at(nid).get();
			}
		
	};
private:
	ReplyMap replies{*this};
public:
	QueryResults(QueryResults&& o)
		:pending_rmap{std::move(o.pending_rmap)},replies{std::move(o.replies)}{}
	QueryResults(const QueryResults&) = delete;
	
	/*
	  Wait the specified duration; if a ReplyMap is available 
	  after that duration, return it. Otherwise return nullptr.
	*/
	template<typename Time>
	ReplyMap* wait(Time t){
		if (replies.rmap.size() == 0){
			if (pending_rmap.wait_for(t) == std::future_status::ready){
				replies.rmap = std::move(*pending_rmap.get());
				return &replies;
			}
			else return nullptr;
		}
		else return &replies;
	}
	
	/*
	  block until the map is fulfilled; then return the map.
	*/
	ReplyMap& get(){
		using namespace std::chrono;
		while (true){
			if (auto rmap = wait(5min)){
				return *rmap;
			}
		}
	}
};
	
template <>
struct QueryResults<void> {
	using type = void;
    /* This currently has no functionality; Ken suggested a "flush," which
       we might want to have in both this and the non-void variant.
    */
};

template <typename T>
struct PendingResults {
    std::promise<std::unique_ptr<reply_map<T> > > pending_map;
    std::map<Node_id, std::promise<T> > populated_promises;

    void fulfill_map(const who_t &who) {
        std::unique_ptr<reply_map<T> > to_add{new reply_map<T>{}};
        for(const auto &e : who) {
            to_add->emplace(e, populated_promises[e].get_future());
        }
        pending_map.set_value(std::move(to_add));
    }

    std::promise<T> &receive_message(const Node_id &nid) {
        return populated_promises[nid];
    }

    QueryResults<T> get_future() {
        return QueryResults<T>{pending_map.get_future()};
    }
};

template <>
struct PendingResults<void> {
    /* This currently has no functionality; Ken suggested a "flush," which
       we might want to have in both this and the non-void variant.
    */

    void fulfill_map(const who_t &) {}
    QueryResults<void> get_future() { return QueryResults<void>{}; }
};

struct remote_exception_occurred : public std::exception{
	Node_id who;
	remote_exception_occurred(Node_id who):who(who){}
};

// many versions of this class will be extended by a single Hanlders context.
// each specific instance of this class provies a mechanism for communicating
// with
// remote sites.

template <FunctionTag tag, typename Ret, typename... Args>
struct RemoteInvocable<tag, Ret(Args...)> {
    using f_t = Ret (*)(Args...);
    const f_t f;
    static const Opcode invoke_id;
    static const Opcode reply_id;

    std::map<std::size_t, PendingResults<Ret> > ret;
    std::mutex ret_lock;
    using lock_t = std::unique_lock<std::mutex>;

    // use this from within a derived class to receive precisely this
    // RemoteInvocable
    //(this way, all RemoteInvocable methods do not need to worry about type
    // collisions)
    inline RemoteInvocable &handler(
        std::integral_constant<FunctionTag, tag> const *const,
        const Args &...) {
        return *this;
    }

    using barray = char *;
    using cbarray = const char *;

    inline auto serialize_one(barray) { return 0; }

    template <typename A, typename... Rest>
    inline auto serialize_one(barray v, const A &a, const Rest &... rest) {
        auto size = mutils::to_bytes(a, v);
        return size + serialize_one(v + size, rest...);
    }

    inline auto serialize_all(barray v, const Args &... args) {
        return serialize_one(v, args...);
    }

    struct send_return {
        std::size_t size;
        char *buf;
        QueryResults<Ret> results;
		PendingResults<Ret> &pending;
    };

    send_return Send(const std::function<char *(int)> &out_alloc,
                     const std::decay_t<Args> &... a) {
        auto invocation_id = mutils::long_rand();
        std::size_t size = mutils::bytes_size(invocation_id);
        {
            auto t = {mutils::bytes_size(a)...};
            size += std::accumulate(t.begin(), t.end(), 0);
        }
        char *serialized_args = out_alloc(size);
        {
            auto v = serialized_args +
                     mutils::to_bytes(invocation_id, serialized_args);
            auto check_size =
                mutils::bytes_size(invocation_id) + serialize_all(v, a...);
            assert(check_size == size);
        }

        lock_t l{ret_lock};
        // default-initialize the maps
        PendingResults<Ret> &pending_results = ret[invocation_id];
		
        return send_return{size, serialized_args, pending_results.get_future(),pending_results};
    }

    template <typename definitely_char>
    inline recv_ret receive_response(
        std::false_type *, mutils::DeserializationManager *dsm,
        const Node_id &who, const char *response,
        const std::function<definitely_char *(int)> &) {
		bool is_exception = response[0];
        long int invocation_id = ((long int *)response + 1)[0];
        assert(ret.count(invocation_id));
        lock_t l{ret_lock};
        // TODO: garbage collection for the responses.
        assert(ret.count(invocation_id));
		if (is_exception){
			ret.at(invocation_id).receive_message(who).set_exception(std::make_exception_ptr(remote_exception_occurred{who}));
		}
		else {
			ret.at(invocation_id)
				.receive_message(who)
				.set_value(*mutils::from_bytes<Ret>(
							   dsm, response + sizeof(invocation_id)));
		}
        return recv_ret{0, 0, nullptr,nullptr};
    }

    inline recv_ret receive_response(std::true_type *,
                                     mutils::DeserializationManager *,
                                     const Node_id &, const char *,
                                     const std::function<char *(int)> &) {
		if (response[0]) throw remote_exception_occurred{who};
        assert(false && "was not expecting a response!");
    }

    inline recv_ret receive_response(mutils::DeserializationManager *dsm,
                                     const Node_id &who, const char *response,
                                     const std::function<char *(int)> &f) {
        constexpr std::is_same<void, Ret> *choice{nullptr};
        return receive_response(choice, dsm, who, response, f);
    }

    std::tuple<> _deserialize(mutils::DeserializationManager *,
                              char const *const) {
        return std::tuple<>{};
    }

    template <typename fst, typename... rst>
    std::tuple<std::unique_ptr<fst>, std::unique_ptr<rst>...> _deserialize(
        mutils::DeserializationManager *dsm, char const *const buf, fst *,
        rst *... rest) {
        using Type = std::decay_t<fst>;
        auto ds = mutils::from_bytes<Type>(dsm, buf);
        const auto size = mutils::bytes_size(*ds);
        return std::tuple_cat(std::make_tuple(std::move(ds)),
                              _deserialize(dsm, buf + size, rest...));
    }

    std::tuple<std::unique_ptr<std::decay_t<Args> >...> deserialize(
        mutils::DeserializationManager *dsm, char const *const buf) {
        return _deserialize(dsm, buf, ((std::decay_t<Args> *)(nullptr))...);
    }

    inline recv_ret receive_call(std::false_type const *const,
                                 mutils::DeserializationManager *dsm,
                                 const Node_id &, const char *_recv_buf,
                                 const std::function<char *(int)> &out_alloc) {
        long int invocation_id = ((long int *)_recv_buf)[0];
        auto recv_buf = _recv_buf + sizeof(long int);
		try{
			const auto result = mutils::callFunc([&](const auto &... a) {
					return f(*a...);
				}, deserialize(dsm, recv_buf));
			// const auto result = f(*deserialize<Args>(dsm, recv_buf)...);
			const auto result_size = mutils::bytes_size(result) + sizeof(long int) + 1;
			auto out = out_alloc(result_size);
			out[0] = false;
			((long int *)(out + 1))[0] = invocation_id;
			mutils::to_bytes(result, out + sizeof(invocation_id) + 1);
			return recv_ret{reply_id, result_size, out,nullptr};
		}
		catch(...){
			//f(*deserialize<Args>(dsm,recv_buf)... );
			char* out = out_alloc(sizeof(long int) + 1);
			out[0]=true;
			((long int*)(out + 1))[0] = invocation_id;
			return recv_ret{reply_id,sizeof(long int) + 1,out,std::current_exception()};
		}
    }

    inline recv_ret receive_call(std::true_type const *const,
                                 mutils::DeserializationManager *dsm,
                                 const Node_id &, const char *_recv_buf,
                                 const std::function<char *(int)> &) {
        auto recv_buf = _recv_buf + sizeof(long int);
        mutils::callFunc([&](const auto &... a) { f(*a...); },
                         deserialize(dsm, recv_buf));
        // f(*deserialize<Args>(dsm, recv_buf)...);
        return recv_ret{reply_id, 0, nullptr};
    }

    inline recv_ret receive_call(mutils::DeserializationManager *dsm,
                                 const Node_id &who, const char *recv_buf,
                                 const std::function<char *(int)> &out_alloc) {
        constexpr std::is_same<Ret, void> *choice{nullptr};
        return this->receive_call(choice, dsm, who, recv_buf, out_alloc);
    }

    RemoteInvocable(std::map<Opcode, receive_fun_t> &receivers,
                    Ret (*f)(Args...))
        : f(f) {
        receivers[invoke_id] =
            [this](auto... a) { return this->receive_call(a...); };
        receivers[reply_id] =
            [this](auto... a) { return this->receive_response(a...); };
    }
};

template <FunctionTag tag, typename Ret, typename... Args>
const Opcode RemoteInvocable<tag, Ret(Args...)>::invoke_id{mutils::gensym()};

template <FunctionTag tag, typename Ret, typename... Args>
const Opcode RemoteInvocable<tag, Ret(Args...)>::reply_id{mutils::gensym()};

template <typename...>
struct RemoteInvocablePairs;

template <FunctionTag id, typename Q>
struct RemoteInvocablePairs<std::integral_constant<FunctionTag, id>, Q>
    : public RemoteInvocable<id, Q> {
    RemoteInvocablePairs(std::map<Opcode, receive_fun_t> &receivers, Q q)
        : RemoteInvocable<id, Q>(receivers, q) {}

    using RemoteInvocable<id, Q>::handler;
};

// id better be an integral constant of Opcode
template <FunctionTag id, typename Q, typename... rest>
struct RemoteInvocablePairs<std::integral_constant<FunctionTag, id>, Q, rest...>
    : public RemoteInvocable<id, Q>, public RemoteInvocablePairs<rest...> {
public:
    template <typename... T>
    RemoteInvocablePairs(std::map<Opcode, receive_fun_t> &receivers, Q q,
                         T &&... t)
        : RemoteInvocable<id, Q>(receivers, q),
          RemoteInvocablePairs<rest...>(receivers, std::forward<T>(t)...) {}

    using RemoteInvocable<id, Q>::handler;
    using RemoteInvocablePairs<rest...>::handler;
};

template <typename... Fs>
struct Handlers : private RemoteInvocablePairs<Fs...> {
private:
    const Node_id nid;
    // listen here
    // LocalMessager my_lm;
    bool alive{true};
    // constructed *before* initialization
    std::unique_ptr<std::map<Opcode, receive_fun_t> > receivers;
    // constructed *after* initialization
    std::unique_ptr<std::thread> receiver;
    mutils::DeserializationManager dsm{{}};

    inline static char *extra_alloc(int i) {
        const auto hs = header_space();
        return (char *)calloc(i + hs, sizeof(char)) + hs;
    }

public:
    inline static auto populate_header(char *reply_buf,
                                       const std::size_t &payload_size,
                                       const Opcode &op, const Node_id &from) {
        ((std::size_t *)reply_buf)[0] = payload_size; // size
        ((Opcode *)(sizeof(std::size_t) + reply_buf))[0] = op;  // what
        ((Node_id *)(sizeof(std::size_t) + sizeof(Opcode) + reply_buf))[0] =
            from;  // from
    }

    inline static auto retrieve_header(mutils::DeserializationManager *dsm,
                                       char const *const reply_buf,
                                       std::size_t &payload_size, Opcode &op,
                                       Node_id &from) {
        payload_size = ((std::size_t const *const)reply_buf)[0];
        op = ((Opcode const *const)(sizeof(std::size_t) + reply_buf))[0];
        from = ((Node_id const *const)(sizeof(std::size_t) + sizeof(Opcode) +
                                       reply_buf))[0];
    }

    inline static auto header_space() {
		return sizeof(std::size_t) + sizeof(Opcode) + sizeof(Node_id);
        //          size           operation           from
    }

	std::exception_ptr handle_receive(const Opcode &indx, const Node_id &received_from,
									  char const * const buf,
									  std::size_t payload_size,
									  const std::function<char *(int)> &out_alloc) {
        using namespace std::placeholders;
        assert(payload_size);
		auto reply_header_size = header_space();
		auto reply_return = receivers->at(indx)(
			&dsm, received_from, buf,
			[&out_alloc, &reply_header_size](std::size_t size) {
				return out_alloc(size + reply_header_size) +
				reply_header_size;
			});
		auto *reply_buf = reply_return.payload;
		if(reply_buf) {
			reply_buf -= reply_header_size;
			const auto id = reply_return.opcode;
			const auto size = reply_return.size;
			populate_header(reply_buf, size, id, nid);
		}
		return reply_return.possible_exception;
    }

	std::exception_ptr handle_receive(char *buf, std::size_t size,
									  const std::function<char *(int)> &out_alloc) {
		std::size_t payload_size;
		Opcode indx;
		Node_id received_from;
		retrieve_header(&dsm, buf, payload_size, indx, received_from);
		return handle_receive(indx,received_from,buf + header_space(),payload_size,out_alloc);
	}

    // these are the functions (no names) from Fs
    template <typename... _Fs>
    Handlers(decltype(receivers) rvrs, Node_id nid, _Fs... fs)
        : RemoteInvocablePairs<Fs...>(*rvrs, fs...),
          nid(nid),
          // my_lm(LocalMessager::init_pipe(nid)),
          receivers(std::move(rvrs)) {
        // receiver.reset(new std::thread{[&]() { receive_call_loop(); }});
    }

    // these are the functions (no names) from Fs
    // delegation so receivers exists during superclass construction
    template <typename... _Fs>
    Handlers(Node_id nid, _Fs... fs)
        : Handlers(
              std::make_unique<typename decltype(receivers)::element_type>(),
              nid, fs...) {}

    ~Handlers() {
        alive = false;
        // receiver->join();
    }
	
	/*
	  much like previous definition, except with 
	  two fewer fields
	*/
	template<typename Ret>
	struct send_return{
		QueryResults<Ret> results;
		PendingResults<Ret> &pending;
	};
	
	/* you *do not* need to delete the pointer in the pair this returns. */
    template <FunctionTag tag, typename... Args>
    auto Send(const std::function<char *(int)>& out_alloc,
              Args &&... args) {
        // this "who" is the destination of this send.
        using namespace std::placeholders;
        constexpr std::integral_constant<FunctionTag, tag> *choice{nullptr};
        auto &hndl = this->handler(choice, args...);
        const auto header_size = header_space();
        auto sent_return =
            hndl.Send([&out_alloc, &header_size](std::size_t size) {
                return out_alloc(size + header_size) + header_size;
            }, std::forward<Args>(args)...);
        std::size_t payload_size = sent_return.size;
        char *buf = sent_return.buf - header_size;
        populate_header(buf, payload_size, hndl.invoke_id, nid);
		using Ret = typename decltype(sent_return.results)::type;
		return send_return<Ret>{std::move(sent_return.results), sent_return.pending};
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
}

using namespace rpc;

// handles up to 5 args
#define HANDLERS_TYPE_ARGS2(a, b) \
    std::integral_constant<rpc::FunctionTag, a>, decltype(b)
#define HANDLERS_TYPE_ARGS4(a, b, c...)                       \
    std::integral_constant<rpc::FunctionTag, a>, decltype(b), \
        HANDLERS_TYPE_ARGS2(c)
#define HANDLERS_TYPE_ARGS6(a, b, c...)                       \
    std::integral_constant<rpc::FunctionTag, a>, decltype(b), \
        HANDLERS_TYPE_ARGS4(c)
#define HANDLERS_TYPE_ARGS8(a, b, c...)                       \
    std::integral_constant<rpc::FunctionTag, a>, decltype(b), \
        HANDLERS_TYPE_ARGS6(c)
#define HANDLERS_TYPE_ARGS10(a, b, c...)                      \
    std::integral_constant<rpc::FunctionTag, a>, decltype(b), \
        HANDLERS_TYPE_ARGS8(c)
#define HANDLERS_TYPE_ARGS_IMPL2(count, ...) \
    HANDLERS_TYPE_ARGS##count(__VA_ARGS__)
#define HANDLERS_TYPE_ARGS_IMPL(count, ...) \
    HANDLERS_TYPE_ARGS_IMPL2(count, __VA_ARGS__)
#define HANDLERS_TYPE_ARGS(...) \
    HANDLERS_TYPE_ARGS_IMPL(VA_NARGS(__VA_ARGS__), __VA_ARGS__)

// handles up to 5 args
#define HANDLERS_ONLY_FUNS2(a, b) b
#define HANDLERS_ONLY_FUNS4(a, b, c...) b, HANDLERS_ONLY_FUNS2(c)
#define HANDLERS_ONLY_FUNS6(a, b, c...) b, HANDLERS_ONLY_FUNS4(c)
#define HANDLERS_ONLY_FUNS8(a, b, c...) b, HANDLERS_ONLY_FUNS6(c)
#define HANDLERS_ONLY_FUNS10(a, b, c...) b, HANDLERS_ONLY_FUNS8(c)
#define HANDLERS_ONLY_FUNS_IMPL2(count, ...) \
    HANDLERS_ONLY_FUNS##count(__VA_ARGS__)
#define HANDLERS_ONLY_FUNS_IMPL(count, ...) \
    HANDLERS_ONLY_FUNS_IMPL2(count, __VA_ARGS__)
#define HANDLERS_ONLY_FUNS(...) \
    HANDLERS_ONLY_FUNS_IMPL(VA_NARGS(__VA_ARGS__), __VA_ARGS__)

#define handlers(m, a...) \
    std::make_unique<Handlers<HANDLERS_TYPE_ARGS(a)> >(m, HANDLERS_ONLY_FUNS(a))
