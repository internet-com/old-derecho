﻿#pragma once

#include "IEquatable.h"
#include <string>
#include <vector>
#include <iostream>
#include <stdexcept>
#include <tchar.h>
#include <memory>

//C# TO C++ CONVERTER NOTE: Forward class declarations:
namespace Derecho { class RDMC; }
namespace Derecho { class SST; }
namespace Derecho { class GMSSST; }


namespace Derecho
{

    class ipAddr : public IEquatable<ipAddr>
    {
  public:
        int theAddr = 0;

        ipAddr(int who);

        int getPid();

        bool Equals(const std::shared_ptr<ipAddr>& someone);

        virtual std::string ToString() override;
    };

    class View : public std::enable_shared_from_this<View>
    {
  public:
        static constexpr int N = 10;
        static int vcntr;
        int ViewInstance = ++vcntr;

        int vid = 0; // Sequential: 0, 1, …
        std::vector<ipAddr> members = std::vector<ipAddr>(N); // Members in view k
        int n = 0; // Number of members
        std::vector<bool> Failed = std::vector<bool>(N); // True if members[i] is considered to have failed
        int nFailed = 0;
        std::shared_ptr<ipAddr> who; // Process that joined or departed since the prior view
        int myRank = 0; // For member p, returns rankOf(p)
        std::vector<RDMC> rdmc = std::vector<RDMC>(N); // One per group member.  Process p sends in rdmc[p];
        std::shared_ptr<SST> gmsSST; // See discussion of gmsSST below

        View();

        void newView(const std::shared_ptr<View>& Vc);

        std::shared_ptr<ipAddr> Joined();

        std::shared_ptr<ipAddr> Departed();
        int RankOfLeader();
        int RankOf(const std::shared_ptr<ipAddr>& who);


        bool IKnowIAmLeader = false; // I am the leader (and know it)

        bool IAmLeader();

        void Destroy();

        virtual std::string ToString() override;
    };

    class Msg : public std::enable_shared_from_this<Msg> // Pass these using smart pointers!
    {
  public:
        int len = 0; // Length in bytes, > 0
        std::vector<unsigned char> body; // Data for this message, sequentially allocated, pinned
    };


    class SST : public std::enable_shared_from_this<SST>
    {
  public:
        static int scntr;
        int SSTInstance = ++scntr;

        int myPid = 0;
        std::vector<GMSSST> row = std::vector<GMSSST>(View::N);

        SST(int vid, int pid, int nRows, std::vector<ipAddr>& pids);

        static void InitializeFromOldSST(const std::shared_ptr<View>& Vnext, const std::shared_ptr<SST>& old, int whichFailed);

        void Push(int myRank, int vid);

        void Pull(const std::shared_ptr<View>& Vc);

        void Freeze(int r);

        void Disable();

        void Enable();

        void Destroy();

        virtual std::string ToString() override;
    };

    class RDMC : public std::enable_shared_from_this<RDMC>
    {
  private:
        bool wedged = false;
        std::vector<bool> frozen = std::vector<bool>(View::N);

  public:
        RDMC(int sender, std::vector<ipAddr>& members, int howMany);

        bool Send(const std::shared_ptr<Msg>& m);

        void Wedge();

        void Destroy();

        void PrepareToReceive(int n);

        void Receive(const std::shared_ptr<Msg>& m);

        static void Destroy(std::vector<RDMC>& rDMC);
    };

    class GMSSST : public std::enable_shared_from_this<GMSSST>
    {
        // Fields used in debugging
  public:
        static int rcntr;
        int gmsSSTRowInstance = ++rcntr;
        int gmsSSTRowTime = 0;
        std::shared_ptr<ipAddr> theIPA; // Saves hassle and simplifies debugging to have this handy
        int vid = 0; // Viewid associated with this SST
        static int maxChanges; // Debugging: largest value nChanges ever had

        volatile std::vector<bool> Suspected = std::vector<bool>(View::N); // Aggregated vector of length |Vc.members|, <= N
        volatile std::vector<ipAddr> Changes = std::vector<ipAddr>(View::N); // A vector of nChanges "proposed changes".   Total number never exceeds N/2
        //      If a request is a Join, Changes[i] in Vc.members
        //      If a request is a Departure, Changes[i] not-in Vc.members
        volatile int nChanges = 0; // How many changes are pending
        volatile int nCommitted = 0; // How many of those have reached the commit point
        volatile int nAcked = 0; // Incremented by a member to acknowledge that it has seen a proposed change.
        volatile std::vector<int> nReceived = std::vector<int>(View::N); // For each sender k, nReceived[k] is the number received (a.k.a. "locally stable")
        volatile bool Wedged = false; // Set after calling RDMC.Wedged(), reports that this member is wedged. Must be after nReceived!
        volatile std::vector<int> GlobalMin = std::vector<int>(View::N); // Vector of how many to accept from each sender, K1-able
        volatile bool GlobalMinReady = false; // Must come after GlobalMin

        GMSSST(int v, const std::shared_ptr<ipAddr>& ipa);

        static int ModIndexer(int n);

        void UseToInitialize(const std::shared_ptr<GMSSST>& newSST);

        void CopyTo(const std::shared_ptr<GMSSST>& destSST);

        virtual std::string ToString() override;
    };

    class Group : public std::enable_shared_from_this<Group>
    {
  public:
        std::string gname;
        std::shared_ptr<View> theView;

        Group(const std::string& gn);

        void SetView(const std::shared_ptr<View>& Vc);

        void Restart(int pid);

        void Leave(const std::shared_ptr<View>& Vc);

  private:
        static volatile std::vector<ipAddr> Joiners;
        static volatile int nJoiners;
        static volatile int JoinsProcessed;

  public:
        void Join(int pid);

  private:
        void SetupSSTandRDMC(int pid, const std::shared_ptr<View>& Vc, std::vector<ipAddr>& pids);

  public:
        bool JoinsPending();

        void ReceiveJoin(const std::shared_ptr<View>& Vc);

        void CommitJoin(const std::shared_ptr<View>& Vc, const std::shared_ptr<ipAddr>& q);

        void ReportFailure(const std::shared_ptr<View>& Vc, const std::shared_ptr<ipAddr>& who);
    };
    class Program : public std::enable_shared_from_this<Program>
    {
        static void Main(std::vector<std::string>& args);

  private:
        static void Launch(const std::shared_ptr<Group>& g, int pid);

        static bool NotEqual(const std::shared_ptr<View>& Vc, std::vector<bool>& old);

        static void Copy(const std::shared_ptr<View>& Vc, std::vector<bool>& old);

        static bool ChangesContains(const std::shared_ptr<View>& Vc, const std::shared_ptr<ipAddr>& q);

        static int MinAcked(const std::shared_ptr<View>& Vc, std::vector<bool>& Failed);

        static bool IAmTheNewLeader(const std::shared_ptr<View>& Vc, const std::shared_ptr<SST>& sst);

        static void Merge(const std::shared_ptr<View>& Vc, int myRank);

  public:
        static void beNode(const std::shared_ptr<Group>& g, int pid);

  private:
        static void WedgeView(const std::shared_ptr<View>& Vc, const std::shared_ptr<SST>& gmsSST, int myRank);

  public:
        static void AwaitMetaWedged(const std::shared_ptr<View>& Vc);

        static int AwaitLeaderGlobalMinReady(const std::shared_ptr<View>& Vc);

        static void DeliverInOrder(const std::shared_ptr<View>& Vc, int Leader);

        static void RaggedEdgeCleanup(const std::shared_ptr<View>& Vc);
    };
}
