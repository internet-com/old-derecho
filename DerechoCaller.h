#pragma once

#include <array>
#include <atomic>
#include <string>
#include <typeinfo>
#include <typeindex>
#include <exception>
#include <unordered_map>
#include <iostream>
#include <functional>
#include <thread>
#include <list>
#include <SerializationSupport.hpp>
#include <FunctionalMap.hpp>
#include <alloca.h>


namespace DerechoCaller
{
	class DerechoCallerException : public std::exception
	{
	public:
		char const * const reason;

		DerechoCallerException(char const * const s):reason(s){}

		virtual const char * what() const throw()
		{
			return reason;
		}

	};


	unsigned int queryID;
	typedef unsigned char Opcode;

	class DerechoHeader : public mutils::ByteRepresentable
	{
	private:
		DerechoHeader(int sid, unsigned int rid, Opcode op, unsigned int tsig)
			:SenderID(sid),replyID(rid),opcode(op),typeSig(tsig){}
	public:
		int SenderID;
		unsigned int replyID;
		Opcode opcode;
		unsigned int typeSig;

		DerechoHeader()
		{}

		DerechoHeader(char const * const where)
			:DerechoHeader(*from_bytes(nullptr,where)) {}

		DerechoHeader(int sid, Opcode op, bool isQuery, bool isReply, int rid, unsigned int tc)
		{
			SenderID = sid;
			opcode = op;
			typeSig = tc;
			if (isQuery)
			{
				replyID = ++queryID;
			}
			else if (isReply)
			{
				replyID = rid;
			}
			else
			{
				replyID = 0;
			}
		}

		static std::unique_ptr<DerechoHeader> from_bytes(mutils::DeserializationManager* p, char const * v){
			auto a2 = mutils::from_bytes<std::decay_t<decltype(SenderID)> >(p,v);
			auto size_a2 = mutils::bytes_size(*a2);
			auto b2 = mutils::from_bytes<std::decay_t<decltype(replyID)> >(p,v + size_a2);
			auto size_b2 = mutils::bytes_size(*b2);
			auto c2 = mutils::from_bytes<std::decay_t<decltype(opcode)> >(p,v + size_a2 + size_b2);
			DerechoHeader r{*a2,*b2,*c2,*(mutils::from_bytes<std::decay_t<decltype(typeSig)> >(p,v + size_a2 + size_b2 + mutils::bytes_size(*c2)))};
			return mutils::heap_copy(r);
		}


		int to_bytes(char* ret) const {
			int sa = mutils::to_bytes(SenderID,ret);
			int sb = mutils::to_bytes(replyID,ret + sa);
			int sc = mutils::to_bytes(opcode,ret + sa + sb);
			return sa + sb + sc + mutils::to_bytes(typeSig,ret + sa + sb + sc);
		}
		int bytes_size() const {
			return mutils::bytes_size(SenderID) + mutils::bytes_size(replyID) + mutils::bytes_size(opcode) + mutils::bytes_size(typeSig);
		}

		void ensure_registered(mutils::DeserializationManager&){}
		//DEFAULT_SERIALIZATION_SUPPORT(DerechoHeader,SenderID,replyID,opcode,typeSig);

	};

	//takes serialized inputs as single char array in first arg,
	//places serialized output in array allocated via second arg
	using cbR = std::pair<std::size_t,char const * const>;
	using CallBack = std::function<cbR(mutils::DeserializationManager*,
									   char const*const,
									   const std::function<char* (int)>&)>;

	class HandlerInfo
	{

	public:
		const unsigned int hc;
		CallBack cb;
		HandlerInfo(unsigned int ahc, CallBack wcb)
			:hc(ahc),cb(wcb){} 
	};

#define common_prefix_dch93_mpm											\
	return [f](mutils::DeserializationManager* dsm,						\
			   char const * const in,									\
			   const std::function<char* (int)>& out_alloc){ \
																		\
		auto mut_in = in;												\
		auto map_fun = [dsm,&mut_in](auto const * const type){			\
			using Type = std::decay_t<decltype(*type)>;					\
			auto ds = mutils::from_bytes<Type>(dsm,mut_in);				\
			const auto size = mutils::bytes_size(*ds);					\
			mut_in += size;												\
			return ds;													\
		};
#define common_suffix_dch93_mpm };

	template<typename R, typename... Args>
	std::enable_if_t<!std::is_same<R,void>::value,CallBack>
	WrappedCallBack(const std::function<R (Args...)> &f){
		common_prefix_dch93_mpm;
        const auto result = f(*map_fun((std::decay_t<Args>*) nullptr)... );
		const auto result_size = mutils::bytes_size(result);
		auto out = out_alloc(result_size);
		mutils::to_bytes(result,out);
		return cbR{mutils::bytes_size(result),out};
		common_suffix_dch93_mpm;
	}
	
	template<typename... Args>
	CallBack WrappedCallBack(const std::function<void (Args...)> &f){
		common_prefix_dch93_mpm;
        f(*map_fun((std::decay_t<Args>*) nullptr)... );
		return cbR{0,nullptr};
		common_suffix_dch93_mpm;
	}

	constexpr unsigned int combine(unsigned int hc1, unsigned int hc2)
	{
		return hc1 ^ (hc2 << 1) | (hc2 >> (sizeof(hc2) * 8 - 1));
	}

	template<typename... T>
	constexpr std::enable_if_t<sizeof...(T) == 0,unsigned int> combine(){
		return 0;
	}

	template<typename T1, typename... T>
	unsigned int combine(){
        return combine(typeid(std::decay_t<T1>).hash_code(),combine<T...>());
	}

	template<typename Ret, typename... Args>
	unsigned int combine_f(std::function<Ret (Args...)> const * const){
		return combine<Ret,Args...>();
	}
	
	template <typename F>
	HandlerInfo Action(const F& pre_cb)
	{
		auto cb = mutils::convert(pre_cb);
		using cb_t = decltype(cb);
		using RT = typename mutils::function_traits<cb_t>::result_type;
		auto hash = combine_f(mutils::mke_p<cb_t>());
		return HandlerInfo(hash, WrappedCallBack(cb));
	}
	
	class _Handler
	{
	public:
		std::unordered_map<unsigned int, HandlerInfo> mytypes;

		_Handler()
		{}

		_Handler& operator+=(const HandlerInfo& hi)
		{
			assert(mytypes.count(hi.hc) == 0);
			mytypes.emplace(hi.hc,hi);
			return *this;
		}

		cbR doCallback(unsigned int hc,
									  mutils::DeserializationManager* dsm,
									  char const * const args,
									  const std::function<char* (int)> &alloc) const 
		{
			return mytypes.at(hc).cb(dsm,args,alloc);
		}
	};

	using Handlers_t = std::unordered_map<std::size_t,_Handler>;
	
	constexpr Opcode REPLY = (Opcode)511;
	constexpr Opcode NULLREPLY = (Opcode)510;
	constexpr Opcode RAW = (Opcode)509;

	template<int SIZE>
	class TransmitInfo
	{
	public:
		bool Match = false;
		const bool isQuery;
		const unsigned int hash;
		const unsigned int replyhc;
        std::vector<char> payload;

		template<typename... Args>
		TransmitInfo(
			const Handlers_t &handlers,
			bool PaxosMode,
			unsigned int ret_hash,
			bool isQuery,
			bool isReply,
			int rid,
			Opcode op,
			const Args &... args)
			:isQuery(isQuery),
			 hash(isQuery ?
				  combine(ret_hash, combine<Args...>()) :
				  (!isReply ?
				   combine(typeid(void).hash_code(), combine<Args...>()) :
				   combine<Args...>()))
			,replyhc(ret_hash) {
				
				//TODO: this thing has a few too many arguments for sanity.  Factoring out should be high priority.
                if (op != REPLY && op != NULLREPLY && handlers.at(op).mytypes.count(hash) == 0)
				{
					std::cerr << "WARNING: Message not sent: No Handler[" << op << "]( " << hash << 
						") has matching reply type and argument types" << std::endl;
				}
				else
				{
					DerechoHeader dh{0, op, isQuery, isReply, rid, hash};
                    auto len = (mutils::bytes_size(args) + ... + mutils::bytes_size(dh));
					Match = true;
                    payload.resize(len);
                    auto dh_size = mutils::to_bytes(dh,payload.data());
                    auto *pp = payload.data() + dh_size;
					auto marshall_args = [&](const auto &arg){ auto size = mutils::to_bytes(arg,pp); pp += size; return size;};
					auto total_size = (marshall_args(args) + ... + mutils::bytes_size(dh));
					assert(total_size == len);
				}
			}

	};

	std::atomic_int nextReplyID{0};

	template<class T>
	class QueryReplies
	{
	public:
		const int replyID = ++nextReplyID;
        std::unique_ptr<T> reply;

        QueryReplies() = default;
        QueryReplies(const QueryReplies&) = delete;

		void gotReply(int who, int rid, std::unique_ptr<T> rep)
		{
            if (who != 0) throw DerechoCallerException("Member replied twice!");
			
			if (rid != replyID)
			{
				std::cerr << "Unexpected reply id in QueryReplies" << std::endl;
                throw DerechoCallerException("QueryReplies");
			}
            if (rep != nullptr){
                reply = std::move(rep);
                std::cout << *reply << std::endl;
            }
		}

		void gotReply(mutils::DeserializationManager* dsm,
					  int who, int rid, char const* const serialized_rep){
			gotReply(who,rid,
					 serialized_rep ?
					 mutils::from_bytes<T>(dsm,serialized_rep) :
					 nullptr
				);
		}

		void nodeFailed(int who)
		{
            /*
			for (auto mi = members.cbegin(); mi != members.cend(); mi++)
			{
				if (*mi == who)
				{
                    members.erase(mi);
					return;
				}
            }*/
		}

		int AwaitReplies()
		{
            return reply ? 1 : 0;//replies.size();
		}
	};

	//grr, global things!
	using replies_map = typename mutils::functional_map<
        int, std::shared_ptr<QueryReplies_general> >;
	using replies_mapnode = typename replies_map::mapnode;

	std::mutex qr_writelock;

	template<typename T>
	using replies_ptr = std::unique_ptr<QueryReplies<T> >;

	template<typename T>
	using replies_promise = std::promise<replies_ptr<T> >;
	template<typename T>
	using replies_future = std::promise<replies_ptr<T> >;

	struct replies_abstract{
		virtual void gotReply(int who, int rid, std::unique_ptr<T> rep) = 0;
	};
	
	template<typename T>
	struct replies_bundle : public replies_abstract{
		replies_ptr<T> reply {new QueryReplies<T>{}};
		replies_promise<T> promise;
		replies_bundle(replies_bundle&&) = default;
		

		void gotReply(int who, int rid, std::unique_ptr<T> rep){
			reply.gotReply(who,rid,rep);
			promise.set_value(std::move(reply));
		}
	};
	
    //std::shared_ptr<replies_mapnode> qrmap{new replies_mapnode{}};
    std::map<int,std::unique_ptr<replies_abstract> > qrmap;
	using qrlock = std::unique_lock<std::mutex>;

	void gotReply(mutils::DeserializationManager* dsm,
				  int senderId, int replyId, unsigned int hc,
				  char const * const payload)
    {
        if (hc != 0)
        {
            const auto &sp = qrmap.at(replyId);//replies_map::find(replyId,*qrmap);
            assert(sp.get());
            if (sp)
                sp->gotReply(dsm,senderId, replyId,payload);
        }
        else
        {
            const auto &sp = qrmap.at(replyId);//replies_map::find(replyId,*qrmap);
            assert(sp.get());
            if (sp.get()) {
                sp->gotReply(nullptr,senderId, replyId,nullptr);
            }
        }

    }

	typedef int NodeId;
	enum { NONE = 0, ALL = -1, MAJORITY = -2 } RepliesNeeded;
	NodeId WHOLEGROUP = -1;
    using NOREPLY = std::unique_ptr<QueryReplies<int> >;

	template<typename Q, int SIZE>
	int _Transmit(mutils::DeserializationManager* dsm,
                  const Handlers_t &Handlers, std::unique_ptr<replies_bundle<Q> > qr_sp, const NodeId who, const TransmitInfo<SIZE>& ti);

	template<int SIZE>
	int TransmitGroup(mutils::DeserializationManager* dsm,
					  const Handlers_t &Handlers, const TransmitInfo<SIZE>& ti) 
	{ 
        return _Transmit<int,SIZE>(dsm,Handlers,NOREPLY{}, WHOLEGROUP, ti);
	}

	template<int SIZE>
	int TransmitP2P(mutils::DeserializationManager* dsm,
					const Handlers_t &Handlers, const NodeId who, const TransmitInfo<SIZE>& ti) 
	{ 
        return _Transmit<int,SIZE>(dsm,Handlers,NOREPLY{}, who, ti);
	}

	template<typename Q, int SIZE>
    int TransmitQuery(mutils::DeserializationManager* dsm, const Handlers_t &Handlers, std::unique_ptr<replies_bundle<Q> > qr, const NodeId who, const TransmitInfo<SIZE>& ti)
	{
        return _Transmit<Q,SIZE>(dsm,Handlers,std::move(qr), who, ti);
	}

    void pretendToDeliver(mutils::DeserializationManager* dsm,
                          const Handlers_t &Handlers,
                          const std::vector<char>&  payload);


	template<typename Q, int SIZE>
	int _Transmit(mutils::DeserializationManager* dsm,
                  const Handlers_t &Handlers, std::unique_ptr<replies_bundle<Q> > qr_up, const NodeId who, const TransmitInfo<SIZE>& ti)
	{
        auto *qr = qr_up.get();
        if (qr != nullptr){
            if (qrmap.count(qr->replyID) == 0/*!replies_map::mem(qr->replyID,*qrmap)*/){
				qrlock l{qr_writelock};
                //qrmap = std::make_shared<replies_mapnode>(replies_map::add(qr->replyID,qr_sp,*qrmap));
                qrmap[qr->replyID] = std::move(qr_up);
			}
			else {
                //replies_map::find(qr->replyID,*qrmap) = qr_sp;
                qrmap.at(qr->replyID) = std::move(qr_up);
			}
            //assert(replies_map::find(qr->replyID,*qrmap) != nullptr);
		}
		if (ti.Match)
		{
            pretendToDeliver(dsm,Handlers,ti.payload);
		}
		if (qr != nullptr)
		{
			return qr->AwaitReplies();
            //replies_map::find(qr->replyID,*qrmap) = nullptr;
		}
		return 0;
	}

	void pretendToDeliver(mutils::DeserializationManager* dsm,
						  const Handlers_t &Handlers,
                          const std::vector<char> &_payload)
	{
		/* Demarshall payload, mimic delivery, etc */
        auto *payload = _payload.data();
        static_assert(sizeof(DerechoHeader) < (sizeof(void*)*10),"bad assumption");
		DerechoHeader dhp(payload);
		payload += mutils::bytes_size(dhp);
		Opcode op = dhp.opcode;
		unsigned int hash = dhp.typeSig;
		if (op == REPLY || op == NULLREPLY)
		{
			gotReply(dsm,dhp.SenderID, dhp.replyID, dhp.typeSig, payload);
			return;
		}
		if (Handlers.at(op).mytypes.count(hash) > 0)
		{
			const HandlerInfo &hi = Handlers.at(op).mytypes.at(hash);
            constexpr auto max_size = 1024;
			char m[max_size];
			auto result_pair = hi.cb(dsm,payload,
								  [&m](int i) -> char* {
									  assert (i < max_size);
									  return m;
								  });
			auto * result = result_pair.second;
			auto result_size = result_pair.first;
			if (dhp.replyID != 0)
			{
				if (result != nullptr)
				{
					/* FAKE Sending reply */
					TransmitP2P<1>(dsm,Handlers,dhp.SenderID, TransmitInfo<1>(Handlers,false, hi.hc, false, true, dhp.replyID, REPLY, mutils::marshalled{result_size,result}));
				}
				else
				{
					TransmitP2P<1>(dsm,Handlers,dhp.SenderID, TransmitInfo<1>(Handlers,false, hi.hc, false, true, dhp.replyID, NULLREPLY));
				}
			}
		}
		else
		{
			unsigned int code = ((unsigned int)op) & 0xFFU;
			std::cerr << "WARNING: There was no handler[" << code << "] with hashcode=" << hash << std::endl;
		}
	}

	template<typename Q>
    void AwaitReplies(const std::unique_ptr<QueryReplies<Q> >&)
	{
		std::cout << "Await Replies: return instantly" << std::endl;
	}

	template <typename... Args>
    void OrderedSend(mutils::DeserializationManager* dsm, const Handlers_t &handlers, Opcode opcode, const Args &... arg)
	{
        static_assert(sizeof(TransmitInfo<sizeof...(Args)>) < sizeof(void*)*10,"that's waay too big");
		TransmitGroup<sizeof...(Args)>(dsm,handlers,
			TransmitInfo<sizeof...(Args)>(
				handlers,false, 0u, false, false, 0U, opcode, arg...));
	}

	template <typename... Args>
    void PaxosSend(mutils::DeserializationManager* dsm, const Handlers_t &handlers, Opcode opcode, const Args &... arg)
	{
		TransmitGroup<sizeof...(Args)>(dsm,handlers,TransmitInfo<sizeof...(Args)>(handlers,true, 0u, false, false, 0U, opcode, arg...));
	}

	template <typename... Args>
    void P2PSend(mutils::DeserializationManager* dsm, const Handlers_t &handlers, NodeId who, Opcode opcode, const Args &... arg)
	{
		TransmitP2P<sizeof...(Args)>(dsm,handlers,who, TransmitInfo<sizeof...(Args)>(handlers,true, 0u, false, false, 0U, opcode, arg...));
	}

	template <typename Q, typename... Args>
    const QueryReplies<Q>& OrderedQuery(mutils::DeserializationManager* dsm, const Handlers_t &handlers, Opcode opcode, const Args &... arg)
	{

        auto *qr = new QueryReplies<Q>();
        TransmitQuery<Q, sizeof...(Args)>(dsm,handlers,replies_ptr<Q>{qr}, WHOLEGROUP, TransmitInfo<sizeof...(Args)>(handlers,false, typeid(Q).hash_code(), true, false, 0U, opcode, arg...));
        return *qr;
	}

	template <typename Q, typename... Args>
    const QueryReplies<Q>& PaxosQuery(mutils::DeserializationManager* dsm, const Handlers_t &handlers, Opcode opcode, const Args &... arg)
    {\
        auto *qr = new QueryReplies<Q>();
        return TransmitQuery<Q, sizeof...(Args)>(dsm,handlers,std::unique_ptr<QueryReplies<Q> >{qr}, WHOLEGROUP, TransmitInfo<sizeof...(Args)>(handlers,true, typeid(Q).hash_code(), true, false, 0U, opcode, arg...));
        return *qr;
	}

	template <typename Q, typename... Args>
    const QueryReplies<Q>& P2PQuery(mutils::DeserializationManager* dsm, const Handlers_t &handlers, NodeId who, Opcode opcode, const Args &... arg)
	{
        auto *qr = new QueryReplies<Q>();
        return TransmitQuery<Q, sizeof...(Args)>(dsm,handlers,std::unique_ptr<QueryReplies<Q> >{qr}, who, TransmitInfo<sizeof...(Args)>(handlers,false, typeid(Q).hash_code(), true, false, 0U, opcode, arg...));
        return *qr;
	}

}
