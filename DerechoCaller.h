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
	using CallBack = std::function<(DeserializationManager*,
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

		void const * const doCallback(unsigned int hc, void const * const args) const 
		{
			return mytypes.at(hc).cb(args);
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
					 Args &... args)
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
				//auto dsi = SendInfo(typeid(DerechoHeader).hash_code(), std::string("DerechoHeader"), false, (vptr *)&dh);
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

	

}
