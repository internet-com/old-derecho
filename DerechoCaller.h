#pragma once

#include <array>
#include <string>
#include <typeinfo>
#include <typeindex>
#include <exception>
#include <unordered_map>
#include <iostream>
#include <functional>
#include <thread>
#include <SerializationSupport.hpp>

namespace DerechoCaller
{
	class DerechoCallerException : public std::exception
	{
	public:
		char *reason;

		DerechoCallerException(char *s)
		{
			reason = s;
		}

		virtual const char* what() const throw()
		{
			return reason;
		}

	};


	unsigned int queryID;
	typedef unsigned char Opcode;

	class DerechoHeader : public ByteRepresentable
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
	using CallBack = std::function<void const * const (DeserializationManager*,
									char const*const,
									const std::function<char const*const (int)>&)>;

	class HandlerInfo
	{

	public:
		const unsigned int hc;
		CallBack cb;
		HandlerInfo(unsigned int ahc, CallBack wcb)
			:hc(ahc),cb(wcb){} 
	};

	template<typename R, typename... Args>
	CallBack WrappedCallBack(const std::function<R (Args...)> &f){
		return [f](DeserializationManager* dsm,
				   void const * const in,
				   const std::function<void const * const (int)>& out_alloc){
			
			auto mut_in = in;
			auto fold_fun = [dsm,&mut_in](auto const * const type){
				using Type = std::decay_t<decltype(*type)>;
				auto ds = mutils::from_bytes<Type>(dsm,mut_in)
				const auto size = ds->bytes_size();
				mut_in += size;
				return ds;
			};
			
			const auto result = f(*fold_fun((Args*) nullptr)... );
			const auto result_size = mutils::bytes_size(result);
			auto out = out_alloc(result_size);
			mutils::to_bytes(result,out);
			return out;
		};
	}

	constexpr unsigned int combine(unsigned int hc1, unsigned int hc2)
	{
		return hc1 ^ (hc2 << 1) | (hc2 >> (sizeof(hc2) * 8 - 1));
	}

	constexpr unsigned int combine(){
		return 0;
	}

	template<typename T1, typename... T>
	constexpr unsigned int combine(){
		return combine(typeid(T1).hash_code(),combine<T...>());
	}

	template<typename Ret, typename... Args>
	constexpr unsigned int combine_f(std::function<Ret (Args...)> const * const){
		return combine<Ret,Args...>();
	}
	
	template <typename F>
	HandlerInfo Action(const F& pre_cb)
	{
		auto cb = mutils::convert(pre_cb);
		using cb_t = decltype(cb);
		using RT = typename mutils::function_traits<cb_t>::result_type;
		constexpr auto hash = combine_f(mutils::mke_p<cb_t>());
		return HandlerInfo(hash, WrappedCallBack(cb));
	}
	
	class _Handler
	{
	public:
		std::unordered_map<unsigned int, HandlerInfo> mytypes;

		_Handler()
		{}

		_Handler& _Handler::operator+=(const HandlerInfo& hi)
		{
			mytypes[hi.hc] = hi;
			return *this;
		}

		void const * const doCallback(unsigned int hc,
									  DeserializationManager* dsm,
									  void const * const args,
									  const std::function<void const * const (int)> &alloc) const 
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
		cptr payload;
		int len;

		template<typename... Args>
		TransmitInfo(
			const Handlers_t &handlers,
					 bool PaxosMode,
					 bool isQuery,
					 bool isReply,
					 int rid,
					 Opcode op,
					 unsigned int ret_hash,
					 const Args &... args)
			:isQuery(isQuery),
			 hash(isQuery ?
				  combine(ret_hash, combine<Args...>()) :
				  (!isReply ?
				   combine(typeid(void).hash_code(), combine<Args...>()) :
				   combine<Args...>()))
		{

 //TODO: this thing has a few too many arguments for sanity.  Factoring out should be high priority.
			if (handlers.at(op).mytypes.count(hash) == 0 && op != REPLY && op != NULLREPLY)
			{
				cerr << "WARNING: Message not sent: No Handler[" << op << "]( " << hash << 
					") has matching reply type and argument types" << endl;
			}
			else
			{
				DerechoHeader dh{0, op, isQuery, isReply, rid, hash};
				len =  mutils::bytes_size(args) + ... + mutils::bytes_size(dh);
				Match = true;
				payload = new char[len];
				cptr pp = payload;
				pp += mutils::to_bytes(dh);
				auto marshall_args = [&](const auto &arg){ auto size = mutils::to_bytes(arg); pp += size; return size;};
				auto total_size = marshall_args(args) + ... + mutils::bytes_size(dh);
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
		virtual void gotReply(DeserializationManager* dsm,
							  int who, int rid, char const* const serialized_rep) = 0;
		virtual ~QueryReplies_general(){}
	};

	template<class T>
	class QueryReplies : public QueryReplies_general
	{
		const int replyID = ++nextReplyID;
		const int repliesWanted;
		std::list<int> members;
	public:
		std::list<std::unique_ptr<T> > replies;
		int repliesReceived;

		QueryReplies(int nreplies):repliesWanted(nreplies){
			members.push_back(0);
		}

		void gotReply(int who, int rid, std::unique_ptr<T> rep)
		{
			
			if (rid != replyID)
			{
				cerr << "Unexpected reply id in QueryReplies" << endl;
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

		void gotReply(DeserializationManager* dsm,
					  int who, int rid, char const* const serialized_rep){
			gotReply(who,rid,
					 serialized_rep ?
					 mutils::from_bytes<T>(dsm,serialized_rep) :
					 nullptr
				);
		}

		void nodeFailed(int who)
		{
			for (mi = members.cbegin; mi != members.cend; mi++)
			{
				if (*mi == who)
				{
					members.remove(mi);
					return;
				}
			}
		}

		int AwaitReplies()
		{
			return repliesReceived;
		}
	};

	std::unordered_map<int, std::unique_ptr<QueryReplies_general> > qrmap;	

	void gotReply(DeserializationManager* dsm,
				  int senderId, int replyId, unsigned int hc, cptr payload)
	{
		try {
			if (hc != 0)
			{
				qrmap.at(replyId)->gotReply(dsm,senderId, replyId,payload);
			}
			else
			{
				qrmap.at(replyId)->gotReply(nullptr,senderId, replyId,nullptr);	
			}
		}
		catch (std::out_of_range&) {return;}
	}

	typedef int NodeId;
	enum { NONE = 0, ALL = -1, MAJORITY = -2 } RepliesNeeded;
	NodeId WHOLEGROUP = -1;
	QueryReplies<int> *NOREPLY = nullptr;

	template<int SIZE>
	int TransmitGroup(const TransmitInfo<SIZE>& ti) 
	{ 
		return _Transmit<int,SIZE>(NOREPLY, WHOLEGROUP, ti); 
	}

	template<int SIZE>
	int TransmitP2P(const NodeId who, const TransmitInfo<SIZE>& ti) 
	{ 
		return _Transmit<int,SIZE>(NOREPLY, who, ti); 
	}

	template<typename Q, int SIZE>
	int TransmitQuery(std::unique_ptr<QueryReplies<Q> > qr, const NodeId who, const TransmitInfo<SIZE>& ti)
	{
		return _Transmit<Q,SIZE>(qr, who, ti);
	}

	template<typename Q, int SIZE>
	int _Transmit(std::unique_ptr<QueryReplies<Q> > qr_up, const NodeId who, const TransmitInfo<SIZE>& ti)
	{
		auto qr = qr_up.get();
		int replyID = 0;
		if (qr != nullptr)
			qrmap[qr->replyID] = std::move(qr_up);
		if (ti.Match)
		{
			pretendToDeliver(ti.payload, ti.len);
		}
		if (qr != nullptr)
		{
			return qr->AwaitReplies();
			qrmap[qr->replyID] = nullptr;
		}
		return 0;
	}

	void pretendToDeliver(DeserializationManager* dsm,
						  const Handlers_t &Handlers,
						  cptr payload, int len)
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
		if (handlers.at(op).mytypes.count(hash) > 0)
		{
			const HandlerInfo &hi = Handlers.at(op).mytypes.at(hash);
			auto * result = hi.cb(dsm,payload,std::alloca);
			if (dhp.replyID != 0)
			{
				if (hi->risPOD || result != nullptr)
				{
					/* FAKE Sending reply */
					TransmitP2P<1>(dhp.SenderID, TransmitInfo<1>(Handlers,false, false, true, dhp.replyID, REPLY, 0, mutils::marshalled(result)));
				}
				else
				{
					TransmitP2P<1>(dhp.SenderID, TransmitInfo<1>(Handlers,false, false, true, dhp.replyID, NULLREPLY, 0));
				}
			}
		}
		else
		{
			unsigned int code = ((unsigned int)op) & 0xFFU;
			cerr << "WARNING: There was no handler[" << code << "] with hashcode=" << hash << endl;
		}
	}

	template<typename Q>
	void AwaitReplies(const QueryReplies<Q> &qr)
	{
		cout << "Await Replies: return instantly" << endl;
	}

	template <typename... Args>
	void OrderedSend(const Handlers_t &handlers, Opcode opcode, Args... arg)
	{
		TransmitGroup<sizeof...(Args)>(
			TransmitInfo<sizeof...(Args)>(
				handlers,false, false, false, opcode, 0U, arg...));
	}

	template <typename... Args>
	void PaxosSend(const Handlers_t &handlers, Opcode opcode, Args... arg)
	{
		TransmitGroup<sizeof...(Args)>(TransmitInfo<sizeof...(Args)>(handlers,true, false, false, opcode, 0U, arg...));
	}

	template <typename... Args>
	void P2PSend(const Handlers_t &handlers, NodeId who, Opcode opcode, Args... arg)
	{
		TransmitP2P<sizeof...(Args)>(who, TransmitInfo<sizeof...(Args)>(handlers,true, false, false, opcode, 0U, arg...));
	}

	template <typename Q, typename... Args>
	int OrderedQuery(const Handlers_t &handlers, QueryReplies<Q> *qr, Opcode opcode, Args... arg)
	{
		return TransmitQuery<Q, sizeof...(Args)>(qr, WHOLEGROUP, TransmitInfo<sizeof...(Args)>(handlers,false, true, false, opcode, 0U, arg...));
	}

	template <typename Q, typename... Args>
	int PaxosQuery(const Handlers_t &handlers, QueryReplies<Q> *qr, Opcode opcode, Args... arg)
	{
		return TransmitQuery<Q, sizeof...(Args)>(qr, WHOLEGROUP, TransmitInfo<sizeof...(Args)>(handlers,true, true, false, opcode, 0U, arg...));
	}

	template <typename Q, typename... Args>
	int P2PQuery(const Handlers_t &handlers, NodeId who, QueryReplies<Q> *qr, Opcode opcode, Args... arg)
	{
		return TransmitQuery<Q, sizeof...(Args)>(qr, who, TransmitInfo<sizeof...(Args)>(handlers,false, true, false, opcode, 0U, arg...));
	}

}
