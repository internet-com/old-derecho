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

		DEFAULT_SERIALIZATION_SUPPORT(DerechoHeader,SenderID,replyID,opcode,typeSig);

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
		const auto result = f(*map_fun((Args*) nullptr)... );
		const auto result_size = mutils::bytes_size(result);
		auto out = out_alloc(result_size);
		mutils::to_bytes(result,out);
		return cbR{mutils::bytes_size(result),out};
		common_suffix_dch93_mpm;
	}
	
	template<typename... Args>
	CallBack WrappedCallBack(const std::function<void (Args...)> &f){
		common_prefix_dch93_mpm;
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
		return combine(typeid(T1).hash_code(),combine<T...>());
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
		char const * payload;
		int len;

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
				if (handlers.at(op).mytypes.count(hash) == 0 && op != REPLY && op != NULLREPLY)
				{
					std::cerr << "WARNING: Message not sent: No Handler[" << op << "]( " << hash << 
						") has matching reply type and argument types" << std::endl;
				}
				else
				{
					DerechoHeader dh{0, op, isQuery, isReply, rid, hash};
					len = (mutils::bytes_size(args) + ... + mutils::bytes_size(dh));
					Match = true;
					auto pp = new char[len];
					payload = pp;
					pp += mutils::to_bytes(dh,pp);
					auto marshall_args = [&](const auto &arg){ auto size = mutils::to_bytes(arg,pp); pp += size; return size;};
					auto total_size = (marshall_args(args) + ... + mutils::bytes_size(dh));
					assert(total_size == len);
				}
			}

		~TransmitInfo()
			{
				if (Match)
				{
					delete[] payload;
				}
			}
	};

	std::atomic_int nextReplyID{0};

	class QueryReplies_general {
	public:
		virtual void gotReply(mutils::DeserializationManager* dsm,
							  int who, int rid, char const* const serialized_rep) = 0;
		virtual ~QueryReplies_general(){}
	};

	template<class T>
	class QueryReplies : public QueryReplies_general
	{
		std::list<int> members;
	public:
		const int replyID = ++nextReplyID;
		const int repliesWanted;
		std::list<std::unique_ptr<T> > replies;
		int repliesReceived;

		QueryReplies(int nreplies):repliesWanted(nreplies){
			members.push_back(0);
		}

		void gotReply(int who, int rid, std::unique_ptr<T> rep)
		{
			
			if (rid != replyID)
			{
				std::cerr << "Unexpected reply id in QueryReplies" << std::endl;
				throw new DerechoCallerException("QueryReplies");
			}

			for (auto const& mi: members)
			{
				if (mi == who)
				{
					members.remove(mi);
					if (rep != nullptr)
					{
						++repliesReceived;
						replies.emplace_back(std::move(rep));
					}
					return;
				}
			}
			throw new DerechoCallerException("Member replied twice!");
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
			for (auto mi = members.cbegin(); mi != members.cend(); mi++)
			{
				if (*mi == who)
				{
					members.remove(*mi);
					return;
				}
			}
		}

		int AwaitReplies()
		{
			return repliesReceived;
		}
	};

	//grr, global things!
	using replies_map = typename mutils::functional_map<
		int, std::weak_ptr<QueryReplies_general> >;
	using replies_mapnode = typename replies_map::mapnode;

	std::mutex qr_writelock;
	std::shared_ptr<replies_mapnode> qrmap{new replies_mapnode{}};
	using qrlock = std::unique_lock<std::mutex>;

	void gotReply(mutils::DeserializationManager* dsm,
				  int senderId, int replyId, unsigned int hc,
				  char const * const payload)
	{
		try {
			if (hc != 0)
			{
				auto sp = replies_map::find(replyId,*qrmap).lock();
				if (sp)
					sp->gotReply(dsm,senderId, replyId,payload);
			}
			else
			{
				auto sp = replies_map::find(replyId,*qrmap).lock();
				if (sp) {
					sp->gotReply(nullptr,senderId, replyId,nullptr);
				}
			}
		}
		catch (const std::out_of_range&) {return;}
	}

	typedef int NodeId;
	enum { NONE = 0, ALL = -1, MAJORITY = -2 } RepliesNeeded;
	NodeId WHOLEGROUP = -1;
	const std::shared_ptr<QueryReplies<int> > NOREPLY{nullptr};

	template<typename Q, int SIZE>
	int _Transmit(mutils::DeserializationManager* dsm,
				  const Handlers_t &Handlers, std::shared_ptr<QueryReplies<Q> > qr_sp, const NodeId who, const TransmitInfo<SIZE>& ti);

	template<int SIZE>
	int TransmitGroup(mutils::DeserializationManager* dsm,
					  const Handlers_t &Handlers, const TransmitInfo<SIZE>& ti) 
	{ 
		return _Transmit<int,SIZE>(dsm,Handlers,NOREPLY, WHOLEGROUP, ti); 
	}

	template<int SIZE>
	int TransmitP2P(mutils::DeserializationManager* dsm,
					const Handlers_t &Handlers, const NodeId who, const TransmitInfo<SIZE>& ti) 
	{ 
		return _Transmit<int,SIZE>(dsm,Handlers,NOREPLY, who, ti); 
	}

	template<typename Q, int SIZE>
	int TransmitQuery(mutils::DeserializationManager* dsm, const Handlers_t &Handlers, std::shared_ptr<QueryReplies<Q> > qr, const NodeId who, const TransmitInfo<SIZE>& ti)
	{
		return _Transmit<Q,SIZE>(dsm,Handlers,qr, who, ti);
	}

	void pretendToDeliver(mutils::DeserializationManager* dsm,
						  const Handlers_t &Handlers,
						  char const *  payload, int len);

	template<typename Q, int SIZE>
	int _Transmit(mutils::DeserializationManager* dsm,
				  const Handlers_t &Handlers, std::shared_ptr<QueryReplies<Q> > qr_sp, const NodeId who, const TransmitInfo<SIZE>& ti)
	{
		auto qr = qr_sp.get();
		int replyID = 0;
		if (qr != nullptr){
			if (replies_map::is_empty(*qrmap)){
				qrlock l{qr_writelock};
				qrmap = std::make_shared<replies_mapnode>(replies_map::add(qr->replyID,qr_sp,*qrmap));
			}
			else {
				replies_map::find(qr->replyID,*qrmap).lock() = qr_sp;
			}
		}
		if (ti.Match)
		{
			pretendToDeliver(dsm,Handlers,ti.payload, ti.len);
		}
		if (qr != nullptr)
		{
			return qr->AwaitReplies();
			replies_map::find(qr->replyID,*qrmap).lock() = nullptr;
		}
		return 0;
	}

	void pretendToDeliver(mutils::DeserializationManager* dsm,
						  const Handlers_t &Handlers,
						  char const *  payload, int len)
	{
		/* Demarshall payload, mimic delivery, etc */
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
			constexpr auto max_size = 1024*1024*1024;
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
	void AwaitReplies(const std::shared_ptr<QueryReplies<Q> >&)
	{
		std::cout << "Await Replies: return instantly" << std::endl;
	}

	template <typename... Args>
	void OrderedSend(mutils::DeserializationManager* dsm, const Handlers_t &handlers, Opcode opcode, Args... arg)
	{
		TransmitGroup<sizeof...(Args)>(dsm,handlers,
			TransmitInfo<sizeof...(Args)>(
				handlers,false, 0u, false, false, 0U, opcode, arg...));
	}

	template <typename... Args>
	void PaxosSend(mutils::DeserializationManager* dsm, const Handlers_t &handlers, Opcode opcode, Args... arg)
	{
		TransmitGroup<sizeof...(Args)>(dsm,handlers,TransmitInfo<sizeof...(Args)>(handlers,true, 0u, false, false, 0U, opcode, arg...));
	}

	template <typename... Args>
	void P2PSend(mutils::DeserializationManager* dsm, const Handlers_t &handlers, NodeId who, Opcode opcode, Args... arg)
	{
		TransmitP2P<sizeof...(Args)>(dsm,handlers,who, TransmitInfo<sizeof...(Args)>(handlers,true, 0u, false, false, 0U, opcode, arg...));
	}

	template <typename Q, typename... Args>
	int OrderedQuery(mutils::DeserializationManager* dsm, const Handlers_t &handlers, std::shared_ptr<QueryReplies<Q> > qr, Opcode opcode, Args... arg)
	{
		return TransmitQuery<Q, sizeof...(Args)>(dsm,handlers,qr, WHOLEGROUP, TransmitInfo<sizeof...(Args)>(handlers,false, typeid(Q).hash_code(), true, false, 0U, opcode, arg...));
	}

	template <typename Q, typename... Args>
	int PaxosQuery(mutils::DeserializationManager* dsm, const Handlers_t &handlers, std::shared_ptr<QueryReplies<Q> > qr, Opcode opcode, Args... arg)
	{
		return TransmitQuery<Q, sizeof...(Args)>(dsm,handlers,qr, WHOLEGROUP, TransmitInfo<sizeof...(Args)>(handlers,true, typeid(Q).hash_code(), true, false, 0U, opcode, arg...));
	}

	template <typename Q, typename... Args>
	int P2PQuery(mutils::DeserializationManager* dsm, const Handlers_t &handlers, NodeId who, std::shared_ptr<QueryReplies<Q> > qr, Opcode opcode, Args... arg)
	{
		return TransmitQuery<Q, sizeof...(Args)>(dsm,handlers,qr, who, TransmitInfo<sizeof...(Args)>(handlers,false, typeid(Q).hash_code(), true, false, 0U, opcode, arg...));
	}

}
