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
		unsigned int hc;
		CallBack cb;
		unsigned int* hcv;
		HandlerInfo(unsigned int ahc, CallBack wcb, unsigned int* hcvec)
			:hc(ahc),cb(wcb),hcv(hcvec){} 
	};

	template <typename RT, typename... Args>
	std::array<unsigned int, (sizeof...(Args))> _Action(RT(*cb)(Args...))
	{
		return{ typeid(Args).hash_code()... };
	}

	template<typename R, typename... Args>
	CallBack WrappedCallBack(const std::function<R>(Args...) &f){
		return [f](DeserializationManager* dsm,
				   void const * const in,
				   const std::function<void const * const (int)>& out_alloc){

			auto fold_fun = [dsm](auto const * const type, const auto& accum){
				using Type = std::decay_t<decltype(*type)>;
				void * in = accum.first;
				auto ds = mutils::from_bytes<Type>(dsm,in)
				const auto size = ds->bytes_size();
				return std::make_pair(in + size,std::tuple_cat(accum.second,std::make_tuple(std::move(ds))));
			};
			
			std::tuple<std::unique_ptr<Args*>...> args{
				mutils::fold(fold_fun,std::tuple<Args*...>{},
							 std::pair<void*,std::tuple<> >{in,std::tuple<>{}}).second
					};
			const auto result = mutils::callFunc(f,args);
			const auto result_size = mutils::bytes_size(result);
			auto out = out_alloc(result_size);
			mutils::to_bytes(result,out);
		};
	}

	template <typename F>
	HandlerInfo Action(const F& pre_cb)
	{
		auto cb = mutils::convert(pre_cb);
		using cb_t = decltype(cb);
		using RT = typename mutils::function_traits<cb_t>::result_type;
		
		return HandlerInfo(typeid(RT).hash_code(), WrappedCallBack(cb),nullptr);
	}
