#include "stdafx.h"
#include "Derecho.h"


namespace Derecho
{

      ipAddr::ipAddr(int who)
      {
          theAddr = who;
      }

      int ipAddr::getPid()
      {
          return theAddr;
      }

      bool ipAddr::Equals(const std::shared_ptr<ipAddr>& someone)
      {
          if (someone == nullptr)
          {
              return false;
          }
          return someone->theAddr == theAddr;
      }

      std::string ipAddr::ToString()
      {
          return std::string(_T("<")) + theAddr + std::string(_T(">"));
      }

int View::vcntr = 0;

      View::View()
      {
      }

      void View::newView(const std::shared_ptr<View>& Vc)
      {
//C# TO C++ CONVERTER TODO TASK: There is no native C++ equivalent to 'ToString':
          std::cout << std::string(_T("Process ")) << Vc->members[Vc->myRank] << std::string(_T("New view: ")) << Vc->ToString() << std::endl;
      }

      std::shared_ptr<ipAddr> View::Joined()
      {
          if (who == nullptr)
          {
              return nullptr;
          }
          for (int r = 0; r < n; r++)
          {
              if (members[r]->Equals(who))
              {
                  return who;
              }
          }
          return nullptr;
      }

      std::shared_ptr<ipAddr> View::Departed()
      {
          if (who == nullptr)
          {
              return nullptr;
          }
          for (int r = 0; r < n; r++)
          {
              if (members[r]->Equals(who))
              {
                  return nullptr;
              }
          }
          return who;
      }

      int View::RankOfLeader()
      {
          for (int r = 0; r < Failed.size(); r++)
          {
              if (!Failed[r])
              {
                  return r;
              }
          }
          return -1;
      }

      int View::RankOf(const std::shared_ptr<ipAddr>& who)
      {
          for (int r = 0; r < n && members[r] != nullptr; r++)
          {
              if (members[r]->Equals(who))
              {
                  return r;
              }
          }
          return -1;
      }

      bool View::IAmLeader()
      {
          return (RankOfLeader() == myRank); // True if I know myself to be the leader
      }

      void View::Destroy()
      {
          for (int n = 0; n < N; n++)
          {
              if (rdmc[n] != nullptr)
              {
                  rdmc[n]->Destroy();
              }
          }
      }

      std::string View::ToString()
      {
          std::string s = std::string(_T("View ")) + vid + std::string(_T(": MyRank=")) + myRank + std::string(_T("... "));
          std::string ms = _T(" ");
          for (int m = 0; m < n; m++)
          {
//C# TO C++ CONVERTER TODO TASK: There is no native C++ equivalent to 'ToString':
              ms += members[m]->ToString() + std::string(_T("  "));
          }

          s += std::string(_T("Members={")) + ms + std::string(_T("}, "));
          std::string fs = _T(" ");
          for (int m = 0; m < n; m++)
          {
              fs += Failed[m] ? _T(" T ") : _T(" F ");
          }

          s += std::string(_T("Failed={")) + fs + std::string(_T(" }, nFailed=")) + nFailed;
          std::shared_ptr<ipAddr> dep = Departed();
          if (dep != nullptr)
          {
              s += std::string(_T(", Departed: ")) + dep;
          }

          std::shared_ptr<ipAddr> join = Joined();
          if (join != nullptr)
          {
              s += std::string(_T(", Joined: ")) + join;
          }

//C# TO C++ CONVERTER TODO TASK: There is no native C++ equivalent to 'ToString':
          s += std::string(_T("\n")) + gmsSST->ToString();
          return s;
      }

int SST::scntr = 0;

      SST::SST(int vid, int pid, int nRows, std::vector<ipAddr>& pids)
      {
          myPid = pid;
          for (int n = 0; n < nRows; n++)
          {
              row[n] = std::make_shared<GMSSST>(vid, pids[n]);
          }
      }

      void SST::InitializeFromOldSST(const std::shared_ptr<View>& Vnext, const std::shared_ptr<SST>& old, int whichFailed)
      {
          int m = 0;
          for (int n = 0; n < Vnext->n && old->row[n] != nullptr; n++)
          {
              if (n != whichFailed)
              {
                  old->row[n]->UseToInitialize(Vnext->gmsSST->row[m++]);
              }
          }
      }

      void SST::Push(int myRank, int vid)
      {
      }

      void SST::Pull(const std::shared_ptr<View>& Vc)
      {
      }

      void SST::Freeze(int r)
      {
          // Freezes row r
      }

      void SST::Disable()
      {
          // Disables rule evaluation, but the SST remains live in the sense that gmsSST.Push(Vc.myRank) still works
          // SST will never be re-enabled
      }

      void SST::Enable()
      {
          // Enables rule evaluation in a new SST instance
      }

      void SST::Destroy()
      {
          // Tear down this SST
      }

      std::string SST::ToString()
      {
          std::string s = _T("SST:\n");
          for (int r = 0; r < row.size(); r++)
          {
              if (row[r] != nullptr)
              {
//C# TO C++ CONVERTER TODO TASK: There is no native C++ equivalent to 'ToString':
                  s += row[r]->ToString();
              }
          }
          return s;
      }

      RDMC::RDMC(int sender, std::vector<ipAddr>& members, int howMany)
      {
          // Set up an RDMC with the designated sender from this list of members.  members[sender] is the ipAddr of the sender
      }

      bool RDMC::Send(const std::shared_ptr<Msg>& m)
      {
          return !wedged;
      }

      void RDMC::Wedge()
      {
          wedged = true;
      }

      void RDMC::Destroy()
      {
          // Tear down connections
      }

      void RDMC::PrepareToReceive(int n)
      {
      }

      void RDMC::Receive(const std::shared_ptr<Msg>& m)
      {
      }

      void RDMC::Destroy(std::vector<RDMC>& rDMC)
      {
      }

int GMSSST::rcntr = 0;
int GMSSST::maxChanges = 0;

      GMSSST::GMSSST(int v, const std::shared_ptr<ipAddr>& ipa)
      {
          vid = v;
          if ((theIPA = ipa) == nullptr)
          {
              throw std::exception(_T("GMSST constructor"));
          }
      }

      int GMSSST::ModIndexer(int n)
      {
          return n % View::N;
      }

      void GMSSST::UseToInitialize(const std::shared_ptr<GMSSST>& newSST)
      {
          Array::Copy(Changes, newSST->Changes, Changes.size());
          newSST->nChanges = nChanges;
          newSST->nCommitted = nCommitted;
          newSST->nAcked = nAcked;
          newSST->gmsSSTRowTime = gmsSSTRowTime;
      }

      void GMSSST::CopyTo(const std::shared_ptr<GMSSST>& destSST)
      {
          if (vid > 0 && destSST->vid > 0 && destSST->vid != vid)
          {
                throw ArgumentOutOfRangeException(std::string(_T("V.")) + destSST->vid + std::string(_T(" attempting to overwrite SST for V.")) + vid);
          }
          if (!destSST->theIPA->Equals(theIPA))
          {
                throw ArgumentOutOfRangeException(std::string(_T("Setting GMSSST row for ")) + destSST->theIPA + std::string(_T(" from the SST row for ")) + theIPA);
          }
          if (destSST->gmsSSTRowTime > gmsSSTRowTime)
          {
                throw ArgumentOutOfRangeException(std::string(_T("Setting GMSSST row for ")) + destSST->theIPA + std::string(_T(" from an OLDER SST row for ")) + theIPA);
          }
          Array::Copy(Suspected, destSST->Suspected, Suspected.size());
          Array::Copy(Changes, destSST->Changes, Changes.size());
          destSST->nChanges = nChanges;
          destSST->nCommitted = nCommitted;
          destSST->nAcked = nAcked;
          Array::Copy(nReceived, destSST->nReceived, nReceived.size());
          if (destSST->Wedged && !Wedged)
          {
                throw ArgumentOutOfRangeException(std::string(_T("Setting instance ")) + destSST->gmsSSTRowInstance + std::string(_T(" Wedged = ")) + Wedged + std::string(_T(" from ")) + gmsSSTRowInstance);
          }
          destSST->Wedged = Wedged;
          Array::Copy(GlobalMin, destSST->GlobalMin, GlobalMin.size());
          destSST->GlobalMinReady = GlobalMinReady;
          destSST->gmsSSTRowTime = gmsSSTRowTime;
      }

      std::string GMSSST::ToString()
      {
          std::string s = theIPA + std::string(_T("@ vid=")) + vid + std::string(_T("[row-time=")) + gmsSSTRowTime + std::string(_T("]: "));
          std::string tf = _T(" ");
          for (int n = 0; n < View::N; n++)
          {
              tf += (Suspected[n]? _T("T"): _T("F")) + std::string(_T(" "));
          }

          s += std::string(_T("Suspected={")) + tf + std::string(_T("}, nChanges=")) + nChanges + std::string(_T(", nCommitted=")) + nCommitted;
          std::string ch = _T(" ");
          for (int n = nCommitted; n < nChanges; n++)
          {
              ch += Changes[GMSSST::ModIndexer(n)];
          }
          std::string rs = _T(" ");
          for (int n = 0; n < nReceived.size(); n++)
          {
              rs += nReceived[n] + std::string(_T(" "));
          }

          s += std::string(_T(", Changes={")) + ch + std::string(_T(" }, nAcked=")) + nAcked + std::string(_T(", nReceived={")) + rs + std::string(_T("}"));
          std::string gs = _T(" ");
          for (int n = 0; n < GlobalMin.size(); n++)
          {
              gs += GlobalMin[n] + std::string(_T(" "));
          }

          s += std::string(_T(", Wedged = ")) + (Wedged? _T("T"): _T("F")) + std::string(_T(", GlobalMin = {")) + gs + std::string(_T("}, GlobalMinReady=")) + GlobalMinReady + std::string(_T("\n"));
          return s;
      }

      Group::Group(const std::string& gn)
      {
          gname = gn;
      }

      void Group::SetView(const std::shared_ptr<View>& Vc)
      {
          theView = Vc;
      }

      void Group::Restart(int pid)
      {
          std::cout << std::string(_T("Process <")) << pid << std::string(_T(">: RESTART Derecho")) << std::endl;
          std::shared_ptr<ipAddr> p = std::make_shared<ipAddr>(pid);
          std::shared_ptr<View> Vc = theView;
          Vc->IKnowIAmLeader = true;
          Vc->members[0] = p;
          Vc->n = 1;
          Vc->vid = 0;
          SetupSSTandRDMC(pid, Vc, Vc->members);
      }

      void Group::Leave(const std::shared_ptr<View>& Vc)
      {
          std::cout << std::string(_T("Process ")) << Vc->members[Vc->myRank]->getPid() << std::string(_T(": Leave Derecho")) << std::endl;
          (std::static_pointer_cast<SST>(Vc->gmsSST))->row[Vc->myRank]->Suspected[Vc->myRank] = true;
          Vc->gmsSST->Push(Vc->myRank, Vc->vid);
      }

volatile std::vector<ipAddr> Group::Joiners = std::vector<ipAddr>(10);
volatile int Group::nJoiners = 0;
volatile int Group::JoinsProcessed = 0;

      void Group::Join(int pid)
      {
          int cnt = 0;
          std::cout << std::string(_T("Process <")) << pid << std::string(_T(">: JOIN Derecho")) << std::endl;
          Joiners[nJoiners] = std::make_shared<ipAddr>(pid);
          nJoiners++;
          while (theView == nullptr)
          {
              delay(2500);
              if (cnt++ == 30)
              {
                  throw std::exception(_T("Join failed"));
              }
          }
          std::shared_ptr<View> Vc = theView;
          std::cout << std::string(_T("Process <")) << pid << std::string(_T(">: JOIN Derecho successful, I was added in view ")) << Vc->vid << std::endl;
          SetupSSTandRDMC(pid, Vc, Vc->members);
          Vc->gmsSST->Pull(Vc);
          Vc->newView(Vc);
      }

      void Group::SetupSSTandRDMC(int pid, const std::shared_ptr<View>& Vc, std::vector<ipAddr>& pids)
      {
          if (Vc->gmsSST != nullptr)
          {
              throw std::exception(_T("Overwritting the SST"));
          }

          Vc->gmsSST = std::make_shared<SST>(Vc->vid, pid, Vc->n, pids);
          for (int sender = 0; sender < Vc->n; sender++)
          {
              Vc->rdmc[sender] = std::make_shared<RDMC>(sender, Vc->members, Vc->n);
          }

          Vc->gmsSST->Enable();
      }

      bool Group::JoinsPending()
      {
          return nJoiners > JoinsProcessed;
      }

      void Group::ReceiveJoin(const std::shared_ptr<View>& Vc)
      {
          std::shared_ptr<ipAddr> q = Joiners[JoinsProcessed++];
          if (q == nullptr)
          {
                throw std::exception(_T("q null in ReceiveJoin"));
          }
          std::shared_ptr<SST> gmsSST = Vc->gmsSST;
          if ((gmsSST->row[Vc->myRank]->nChanges - gmsSST->row[Vc->myRank]->nCommitted) == gmsSST->row[Vc->myRank]->Changes.size())
          {
              throw std::exception(_T("Too many changes to allow a Join right now"));
          }

          gmsSST->row[Vc->myRank]->Changes[GMSSST::ModIndexer(gmsSST->row[Vc->myRank]->nChanges)] = q;
          gmsSST->row[Vc->myRank]->nChanges++;
          if (Vc->gmsSST->row[Vc->myRank]->nChanges > GMSSST::maxChanges)
          {
              GMSSST::maxChanges = Vc->gmsSST->row[Vc->myRank]->nChanges;
          }
          for (int n = 0; n < Vc->n; n++)
          {
              Vc->rdmc[n]->Wedge(); // RDMC finishes sending, then stops sending or receiving in Vc
          }
          gmsSST->row[Vc->myRank]->Wedged = true; // True if RDMC has halted new sends and receives in Vc
          gmsSST->Push(Vc->myRank, Vc->vid);
      }

      void Group::CommitJoin(const std::shared_ptr<View>& Vc, const std::shared_ptr<ipAddr>& q)
      {
          std::cout << std::string(_T("CommitJoin: Vid=")) << Vc->vid << std::string(_T("... joiner is ")) << q << std::endl;
          std::make_shared<Thread>([&] ()
              // Runs in a separate thread in case state transfer is (in the future) at all slow
              // EDWARD TO DO //
          {
                std::shared_ptr<View> Vcp = std::make_shared<View>();
                Vcp->vid = Vc->vid;
                Array::Copy(Vc->members, Vcp->members, Vc->n);
                Vcp->n = Vc->n;
                Vcp->myRank = Vc->RankOf(q);
                Vcp->who = Vcp->members[Vcp->myRank];
                Array::Copy(Vc->Failed, Vcp->Failed, Vc->Failed.size());
                Vcp->nFailed = Vc->nFailed;
                std::cout << std::string(_T("Sending View ")) << Vcp->vid << std::string(_T(" to ")) << q << std::endl;
          }).Start();
      }

      void Group::ReportFailure(const std::shared_ptr<View>& Vc, const std::shared_ptr<ipAddr>& who)
      {
          int r = Vc->RankOf(who);
          (std::static_pointer_cast<SST>(Vc->gmsSST))->row[Vc->myRank]->Suspected[Vc->myRank] = true;
          Vc->gmsSST->Push(Vc->myRank, Vc->vid);
      }

      void Program::Main(std::vector<std::string>& args)
      {
          std::shared_ptr<Group> g = std::make_shared<Group>(_T("Derecho-Test"));
          for (int pid = 0; pid < 10; pid++)
          {
              Launch(g, pid);
          }
      }

      void Program::Launch(const std::shared_ptr<Group>& g, int pid)
      {
          std::shared_ptr<Thread> tempVar = std::make_shared<Thread>([&] ()
                      /* Restart the system */
          {
                if (pid == 0)
                {
                      g->SetView(std::make_shared<View>());
                      g->Restart(pid);
                      beNode(g, pid);
                }
                else
                {
                      delay(pid * 5000);
                      g->Join(pid);
                      beNode(g, pid);
                }
                std::cout << std::string(_T("TERMINATION: Pid<")) << pid << std::string(_T(">")) << std::endl;
          });
          tempVar->Name = std::string(_T("Process ")) + pid;
          std::shared_ptr<Thread> t = tempVar;
          t->Start();
      }

      bool Program::NotEqual(const std::shared_ptr<View>& Vc, std::vector<bool>& old)
      {
          for (int r = 0; r < Vc->n; r++)
          {
              for (int who = 0; who < View::N; who++)
              {
                  if (Vc->gmsSST->row[r]->Suspected[who] && !old[who])
                  {
                      return true;
                  }
              }
          }
          return false;
      }

      void Program::Copy(const std::shared_ptr<View>& Vc, std::vector<bool>& old)
      {
          int myRank = Vc->myRank;
          for (int who = 0; who < Vc->n; who++)
          {
              old[who] = Vc->gmsSST->row[myRank]->Suspected[who];
          }
      }

      bool Program::ChangesContains(const std::shared_ptr<View>& Vc, const std::shared_ptr<ipAddr>& q)
      {
          std::shared_ptr<GMSSST> myRow = Vc->gmsSST->row[Vc->myRank];
          for (int n = myRow->nCommitted; n < myRow->nChanges; n++)
          {
              std::shared_ptr<ipAddr> p = myRow->Changes[GMSSST::ModIndexer(n)];
              if (p != nullptr && p->Equals(q))
              {
                  return true;
              }
          }
          return false;
      }

      int Program::MinAcked(const std::shared_ptr<View>& Vc, std::vector<bool>& Failed)
      {
          int myRank = Vc->myRank;
          int min = Vc->gmsSST->row[myRank]->nAcked;
          for (int n = 0; n < Vc->n; n++)
          {
              if (!Failed[n] && Vc->gmsSST->row[n]->nAcked < min)
              {
                  min = Vc->gmsSST->row[n]->nAcked;
              }
          }

          return min;
      }

      bool Program::IAmTheNewLeader(const std::shared_ptr<View>& Vc, const std::shared_ptr<SST>& sst)
      {
          if (Vc->IKnowIAmLeader)
          {
              return false; // I am the OLD leader
          }

          for (int n = 0; n < Vc->myRank; n++)
          {
              for (int row = 0; row < Vc->myRank; row++)
              {
                  if (!Vc->Failed[n] && !Vc->gmsSST->row[row]->Suspected[n])
                  {
                      return false; // I'm not the new leader, or some failure suspicion hasn't fully propagated
                  }
              }
          }
          Vc->IKnowIAmLeader = true;
          return true;
      }

      void Program::Merge(const std::shared_ptr<View>& Vc, int myRank)
      {
          // Merge the change lists
          for (int n = 0; n < Vc->n; n++)
          {
              if (Vc->gmsSST->row[myRank]->nChanges < Vc->gmsSST->row[n]->nChanges)
              {
                  Array::Copy(Vc->gmsSST->row[n]->Changes, Vc->gmsSST->row[myRank]->Changes, Vc->gmsSST->row[myRank]->Changes.size());
                  Vc->gmsSST->row[myRank]->nChanges = Vc->gmsSST->row[n]->nChanges;
                  if (Vc->gmsSST->row[myRank]->nChanges > GMSSST::maxChanges)
                  {
                      GMSSST::maxChanges = Vc->gmsSST->row[myRank]->nChanges;
                  }
              }

              if (Vc->gmsSST->row[myRank]->nCommitted < Vc->gmsSST->row[n]->nCommitted) // How many I know to have been committed
              {
                  Vc->gmsSST->row[myRank]->nCommitted = Vc->gmsSST->row[n]->nCommitted;
              }
          }
          bool found = false;
          for (int n = 0; n < Vc->n; n++)
          {
              if (Vc->Failed[n])
              {
                  // Make sure that the failed process is listed in the Changes vector as a proposed change
                  for (int c = Vc->gmsSST->row[myRank]->nCommitted; c < Vc->gmsSST->row[myRank]->nChanges && !found; c++)
                  {
                      if (Vc->gmsSST->row[myRank]->Changes[GMSSST::ModIndexer(c)]->Equals(Vc->members[n]))
                      {
                          // Already listed
                          found = true;
                      }
                  }
              }
              else
              {
                  // Not failed
                  found = true;
              }

              if (!found)
              {
                  Vc->gmsSST->row[myRank]->Changes[GMSSST::ModIndexer(Vc->gmsSST->row[myRank]->nChanges)] = Vc->members[n];
                  Vc->gmsSST->row[myRank]->nChanges++;
                  if (Vc->gmsSST->row[myRank]->nChanges > GMSSST::maxChanges)
                  {
                      GMSSST::maxChanges = Vc->gmsSST->row[myRank]->nChanges;
                  }
              }
          }
          Vc->gmsSST->Push(Vc->myRank, Vc->vid);
      }

      void Program::beNode(const std::shared_ptr<Group>& g, int pid)
      {
          std::shared_ptr<ipAddr> myAddr = std::make_shared<ipAddr>(pid);
          std::vector<bool> oldSuspected = std::vector<bool>(View::N);

          while (true)
          {
              std::shared_ptr<View> Vc = g->theView;
              std::shared_ptr<SST> gmsSST = Vc->gmsSST;
              int Leader = Vc->RankOfLeader(), myRank = Vc->myRank;

              if (NotEqual(Vc, oldSuspected))
              {
                  // Aggregate suspicions into gmsSST[myRank].Suspected;
                  for (int r = 0; r < Vc->n; r++)
                  {
                      for (int who = 0; who < Vc->n; who++)
                      {
                          gmsSST->row[myRank]->Suspected[who] |= gmsSST->row[r]->Suspected[who];
                      }
                  }

                  for (int q = 0; q < Vc->n; q++)
                  {
                      if (gmsSST->row[myRank]->Suspected[q] && !Vc->Failed[q])
                      {
                          if (Vc->nFailed + 1 >= View::N / 2)
                          {
                              throw std::exception(_T("Majority of a Derecho group simultaneously failed … shutting down"));
                          }

                          gmsSST->Freeze(q); // Cease to accept new updates from q
                          for (int n = 0; n < Vc->n; n++)
                          {
                              Vc->rdmc[n]->Wedge(); // RDMC finishes sending, then stops sending or receiving in Vc
                          }

                          gmsSST->row[myRank]->Wedged = true; // RDMC has halted new sends and receives in Vc
                          Vc->Failed[q] = true;
                          Vc->nFailed++;

                          if (Vc->nFailed > Vc->n / 2 || (Vc->nFailed == Vc->n / 2 && Vc->n % 2 == 0))
                          {
                              throw std::exception(_T("Potential partitioning event: this node is no longer in the majority and must shut down!"));
                          }

                          gmsSST->Push(Vc->myRank, Vc->vid);
                          if (Vc->IAmLeader() && !ChangesContains(Vc, Vc->members[q])) // Leader initiated
                          {
                              if ((gmsSST->row[myRank]->nChanges - gmsSST->row[myRank]->nCommitted) == gmsSST->row[myRank]->Changes.size())
                              {
                                  throw std::exception(_T("Ran out of room in the pending changes list"));
                              }

                              gmsSST->row[myRank]->Changes[GMSSST::ModIndexer(gmsSST->row[myRank]->nChanges)] = Vc->members[q]; // Reports the failure (note that q NotIn members)
                              gmsSST->row[myRank]->nChanges++;
                              std::cout << std::string(_T("NEW SUSPICION: adding ")) << Vc->members[q] << std::string(_T(" to the CHANGES/FAILED list")) << std::endl;
                              if (gmsSST->row[myRank]->nChanges > GMSSST::maxChanges)
                              {
                                  GMSSST::maxChanges = gmsSST->row[myRank]->nChanges;
                              }
                              gmsSST->Push(Vc->myRank, Vc->vid);
                          }
                      }
                  }
                  Copy(Vc, oldSuspected);
              }

              if (Vc->IAmLeader() && g->JoinsPending())
              {
                  g->ReceiveJoin(Vc);
              }

              int M;
              if (myRank == Leader && (M = MinAcked(Vc, Vc->Failed)) > gmsSST->row[myRank]->nCommitted)
              {
                  gmsSST->row[myRank]->nCommitted = M; // Leader commits a new request
                  gmsSST->Push(Vc->myRank, Vc->vid);
              }

              if (gmsSST->row[Leader]->nChanges > gmsSST->row[myRank]->nAcked)
              {
                  WedgeView(Vc, gmsSST, myRank); // True if RDMC has halted new sends, receives in Vc
                  if (myRank != Leader)
                  {
                      Array::Copy(gmsSST->row[Leader]->Changes, gmsSST->row[myRank]->Changes, gmsSST->row[myRank]->Changes.size()); // Echo (copy) the vector including the new changes
                      gmsSST->row[myRank]->nChanges = gmsSST->row[Leader]->nChanges; // Echo the count
                      gmsSST->row[myRank]->nCommitted = gmsSST->row[Leader]->nCommitted;
                  }

                  gmsSST->row[myRank]->nAcked = gmsSST->row[Leader]->nChanges; // Notice a new request, acknowledge it
                  gmsSST->Push(Vc->myRank, Vc->vid);
              }

              if (gmsSST->row[Leader]->nCommitted > Vc->vid)
              {
                  gmsSST->Disable(); // Disables the SST rule evaluation for this SST
                  WedgeView(Vc, gmsSST, myRank);
                  std::shared_ptr<ipAddr> q = gmsSST->row[myRank]->Changes[GMSSST::ModIndexer(Vc->vid)];
                  std::shared_ptr<View> Vnext = std::make_shared<View>();
                  Vnext->vid = Vc->vid + 1;
                  Vnext->IKnowIAmLeader = Vc->IKnowIAmLeader;
                  std::shared_ptr<ipAddr> myIPAddr = Vc->members[myRank];
                  bool failed;
                  int whoFailed = Vc->RankOf(q);
                  if (whoFailed != -1)
                  {
                      failed = true;
                      Vnext->nFailed = Vc->nFailed - 1;
                      Vnext->n = Vc->n - 1;
                  }
                  else
                  {
                      failed = false;
                      Vnext->nFailed = Vc->nFailed;
                      Vnext->n = Vc->n;
                      Vnext->members[Vnext->n++] = q;
                  }

                  int m = 0;
                  for (int n = 0; n < Vc->n; n++)
                  {
                      if (n != whoFailed)
                      {
                          Vnext->members[m] = Vc->members[n];
                          Vnext->Failed[m] = Vc->Failed[n];
                          ++m;
                      }
                  }

                  Vnext->who = q;
                  if ((Vnext->myRank = Vnext->RankOf(myIPAddr)) == -1)
                  {
                      std::cout << std::string(_T("Some other process reported that I failed.  Process ")) << myIPAddr << std::string(_T(" terminating")) << std::endl;
                      return;
                  }

                  if (Vnext->gmsSST != nullptr)
                  {
                      throw std::exception(_T("Overwritting the SST"));
                  }

                  Vc->gmsSST->Pull(Vc);
                  Vnext->gmsSST = std::make_shared<SST>(Vnext->vid, Vc->gmsSST->myPid, Vnext->n, Vnext->members);
                  SST::InitializeFromOldSST(Vnext, gmsSST, whoFailed);
                  Vnext->gmsSST->Pull(Vnext);

                  // The intent of these next lines is that we move entirely to Vc+1 and cease to use Vc
                  RaggedEdgeCleanup(Vc); // Finalize deliveries in Vc

                  for (int sender = 0; sender < Vnext->n; sender++)
                  {
                      Vnext->rdmc[sender] = std::make_shared<RDMC>(sender, Vnext->members, Vnext->n);
                  }

                  if (Vc->IAmLeader() && !failed)
                  {
                      g->CommitJoin(Vnext, q);
                  }

                  Vnext->gmsSST->Enable();
                  Vnext->gmsSST->Push(Vnext->myRank, Vc->vid);

                  std::shared_ptr<View> oldView = Vc;
                  Vc = Vnext;
                  gmsSST = Vc->gmsSST;
                  Vc->newView(Vnext); // Announce the new view to the application

                  // Finally, some cleanup.  These could be in a background thread
                  RDMC::Destroy(oldView->rdmc); // The old RDMC instance is no longer active
                  oldView->gmsSST->Destroy(); // The old SST instance can be discarded too
                  oldView->Destroy(); // VC no longer active

                  // First task with my new view...
                  if (IAmTheNewLeader(Vc, gmsSST)) // I’m the new leader and everyone who hasn’t failed agrees
                  {
                      Merge(Vc, Vc->myRank); // Create a combined list of Changes
                  }
              }
          }
      }

      void Program::WedgeView(const std::shared_ptr<View>& Vc, const std::shared_ptr<SST>& gmsSST, int myRank)
      {
          for (int n = 0; n < Vc->n; n++)
          {
              Vc->rdmc[n]->Wedge(); // RDMC finishes sending, stops new sends or receives in Vc
          }

          gmsSST->row[myRank]->Wedged = true;
      }

      void Program::AwaitMetaWedged(const std::shared_ptr<View>& Vc)
      {
          int cnt = 0;
          for (int n = 0; n < Vc->n; n++)
          {
              while (!Vc->Failed[n] && !Vc->gmsSST->row[n]->Wedged)
              {
                  /* busy-wait */
                  if (cnt++ % 100 == 0)
                  {
                      std::cout << std::string(_T("Process ")) << Vc->members[Vc->myRank] << std::string(_T("... loop in AwaitMetaWedged / ")) << Vc->gmsSST->row[n]->gmsSSTRowInstance << std::endl;
                  }

                  delay(10);
                  Vc->gmsSST->Pull(Vc);
              }
          }
      }

      int Program::AwaitLeaderGlobalMinReady(const std::shared_ptr<View>& Vc)
      {
          int Leader = Vc->RankOfLeader();
          while (!Vc->gmsSST->row[Leader]->GlobalMinReady)
          {
              Leader = Vc->RankOfLeader();
              Vc->gmsSST->Pull(Vc);
          }
          return Leader;
      }

      void Program::DeliverInOrder(const std::shared_ptr<View>& Vc, int Leader)
      {
          // Ragged cleanup is finished, deliver in the implied order
          std::string deliveryOrder = std::string(_T("Delivery Order (View ")) + Vc->vid + std::string(_T(") { "));
          for (int n = 0; n < Vc->n; n++)
          {
              deliveryOrder += Vc->gmsSST->myPid + std::string(_T(":0..")) + Vc->gmsSST->row[Leader]->GlobalMin[n] + std::string(_T(" "));
          }

          std::cout << deliveryOrder << std::string(_T("}")) << std::endl;
      }

      void Program::RaggedEdgeCleanup(const std::shared_ptr<View>& Vc)
      {
          AwaitMetaWedged(Vc);
          int myRank = Vc->myRank;
          // This logic depends on the transitivity of the K1 operator, as discussed last week
          int Leader = Vc->RankOfLeader(); // We don’t want this to change under our feet
          if (Vc->IAmLeader())
          {
              std::cout << std::string(_T("Running RaggedEdgeCleanup: ")) << Vc << std::endl;
              bool found = false;
              Vc->gmsSST->Pull(Vc);
              for (int n = 0; n < Vc->n && !found; n++)
              {
                  if (Vc->gmsSST->row[n]->GlobalMinReady)
                  {
                      Array::Copy(Vc->gmsSST->row[myRank]->GlobalMin, Vc->gmsSST->row[n]->GlobalMin, Vc->n);
                      found = true;
                  }
              }

              if (!found)
              {
                  for (int n = 0; n < Vc->n; n++)
                  {
                      int min = Vc->gmsSST->row[myRank]->nReceived[n];
                      for (int r = 0; r < Vc->n; r++)
                      {
                          if (!Vc->Failed[r] && min > Vc->gmsSST->row[r]->nReceived[n])
                          {
                              min = Vc->gmsSST->row[r]->nReceived[n];
                          }
                      }

                      Vc->gmsSST->row[myRank]->GlobalMin[n] = min;
                  }
              }

              Vc->gmsSST->row[myRank]->GlobalMinReady = true;
              Vc->gmsSST->Push(Vc->myRank, Vc->vid);
              std::cout << std::string(_T("RaggedEdgeCleanup: FINAL = ")) << Vc << std::endl;
          }
          else
          {
              // Learn the leader’s data and push it before acting upon it
              Leader = AwaitLeaderGlobalMinReady(Vc);
              Array::Copy(Vc->gmsSST->row[myRank]->GlobalMin, Vc->gmsSST->row[Leader]->GlobalMin, Vc->n);
              Vc->gmsSST->row[myRank]->GlobalMinReady = true;
              Vc->gmsSST->Push(Vc->myRank, Vc->vid);
          }

          DeliverInOrder(Vc, Leader);
      }
}
