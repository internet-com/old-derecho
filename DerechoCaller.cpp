// DerechoCaller.cpp : Defines the entry point for the console application.
//

#include "DerechoCaller.h"

using namespace DerechoCaller;
using namespace mutils;
using namespace std;

enum
{
	HELLO = 0,
	GOODBYE = 1
};

class Foo: public ByteRepresentable
{
public:
	int bar;

	Foo() { cout << "Foo: null constructor" << endl; }

	Foo(int b)
	{
		cout << "Foo(" << b << ")" << endl;
		bar = b;
	}

	DEFAULT_SERIALIZATION_SUPPORT(Foo,bar);	
};

int icallWithInt(int x)
{
	cout << "iCalled with int x = " << x << endl;
	return x-1;
}

void vcallWithInt(int x)
{
	cout << "vCalled with int x = " << x << endl;
}

void callWith2Ints(int x, int y)
{ 
	cout << "callWith2Ints Called with int x = " << x << ", y = " << y << endl; 
}

void callWith3Ints(int x, int y, int z)
{ 
	cout << "callWith3Ints Called with int x = " << x << ", y=" << y << ", z=" << z << endl; 
}

double dcallWithDouble(double x)
{
	cout << "dCalled with double x = " << x << endl;
	return 0.0;
}

void vcallWithDouble(double x)
{
	cout << "vCalled with double x = " << x << endl;
}

double fcallWithFloat(float x)
{
	cout << "dCalled with float x = " << x << endl;
	return 0.0;
}

void vcallWithFloat(float x)
{
	cout << "vCalled with float x = " << x << endl;
}

void callWithFoo(const Foo &x)
{
	cout << "Called with Foo x.bar = " << x.bar << endl;
}

void callWithIntFoo(int x, const Foo& y)
{
	cout << "Called with int x = " << x << ", and with Foo y.bar = " << y.bar << endl;
}

void callWithFooInt(const Foo& x, int y)
{
	cout << "Called with Foo x = " << x.bar << ", and with int y = " << y << endl;
}

void callWithNothing()
{
	cout << "Called with nothing" << endl;
}

template<typename T>
void ReportOutcome(const QueryReplies<T> &qr)
{
	cout << "ReportingOutcome: nr=" << qr.repliesReceived << ", replies[ ";
	for (const auto &ri : qr.replies)
		cout << *ri << " ";
	cout << "]" << endl;
}

void runTest()
{
	Handlers_t Handlers;
	Handlers[HELLO] += Action(callWithNothing);		// <void>
	Handlers[HELLO] += Action(icallWithInt);        // <int,int>
	Handlers[HELLO] += Action(vcallWithInt);		// <void, int>
	Handlers[HELLO] += Action(callWith2Ints);		// <void,int, int>
	Handlers[HELLO] += Action(callWith3Ints);		// <void, int, int, int>
	//Handlers[HELLO] += Action<void, int>([](int x) -> void { cout << "[1]Called with int x = " << x << endl; });
	//Handlers[HELLO] += Action<void,int, int>([](int x, int y) -> void { cout << "[2]Called with int x = " << x << ", y = " << y <<  endl; });
	//Handlers[HELLO += Action<void,int,int,int>([](int x, int y, int z) -> void { cout << "[3]Called with int x = " << x << ", y=" << y << ", z=" << z << endl; });
	Handlers[HELLO] += Action(fcallWithFloat);		// <double, double>
	Handlers[HELLO] += Action(vcallWithFloat);		// <void, double>
	Handlers[HELLO] += Action(dcallWithDouble);		// <double, double>
	Handlers[HELLO] += Action(vcallWithDouble);		// <void, double>
	Handlers[HELLO] += Action(callWithFoo);			// <void, Foo*>
	Handlers[HELLO] += Action(callWithIntFoo);		// <void, int, Foo*>
	Handlers[HELLO] += Action(callWithFooInt);		// <void, Foo*, int>
	//Handlers[GOODBYE] += Action([](int n) -> void { cout << "[GOODBYE] was queried, n=" << n << endl;  }); <void, int>
	DeserializationManager* dsm{nullptr};
	for (int n = 0; n < 10; n++)
	{
		cout << "OrderedSend(HELLO)" << endl;
		OrderedSend(dsm,Handlers,HELLO);
		cout << "OrderedSend(HELLO, 1)" << endl;
		OrderedSend(dsm,Handlers,HELLO, 1);
		cout << "OrderedSend(HELLO, 1, 2)" << endl;
		OrderedSend(dsm,Handlers,HELLO, 1, 2);
		cout << "OrderedSend(HELLO, 1, 2, 3)" << endl;
		OrderedSend(dsm,Handlers,HELLO, 1, 2, 3);
		cout << "OrderedSend(HELLO, float 5.4321)" << endl;
		OrderedSend(dsm,Handlers,HELLO, (float)5.4321);
		cout << "OrderedSend(HELLO, double 1.234)" << endl;
		OrderedSend(dsm,Handlers,HELLO, 1.234);
		Foo f(9876 + n);
		cout << "OrderedSend(HELLO, Foo*)" << endl;
		OrderedSend(dsm,Handlers,HELLO, f);
		cout << "OrderedSend(HELLO, 1, (float)22.4, Foo*)" << endl;
		OrderedSend(dsm,Handlers,HELLO, 1, 22.4, f);
		cout << "OrderedSend(HELLO, 1, 22.4, ``string'')" << endl;
		OrderedSend(dsm,Handlers,HELLO, 1, 22.4, "string of some sort");
		OrderedSend(dsm,Handlers,HELLO, 22.4, 21, "string of some sort", f, 77);
		cout << "Query(HELLO)" << endl;
		int nr;
		{
			QueryReplies<int> qr(ALL);
			std::shared_ptr<QueryReplies<int> > qrp(&qr,[](const auto&){});
			nr = OrderedQuery(dsm,Handlers,qrp, HELLO);
			ReportOutcome<int>(qr);
		}
		{
			cout << "Query(HELLO,9)" << endl;
			QueryReplies<int> qr(ALL);
			std::shared_ptr<QueryReplies<int> > qrp(&qr,[](const auto&){});
			nr = OrderedQuery(dsm,Handlers,qrp, HELLO, 9);
			ReportOutcome<int>(qr);
		}
	}
}

int main()
{
	runTest();
	cout << "Hello world... enter any character to terminate" << endl;
	string myString;
	getline(cin, myString);
	return 0;
}
