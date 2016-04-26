/*
 * view_utils.h
 *
 *  Created on: Apr 25, 2016
 *      Author: edward
 */

#ifndef VIEW_UTILS_H_
#define VIEW_UTILS_H_

#include <vector>

#include "view.h"

namespace derecho {

bool NotEqual(const View& theView, const std::vector<bool>& old);

void Copy(const View& Vc, std::vector<bool>& old);

bool ChangesContains(const View& Vc, const ip_addr& q);

int MinAcked(const View& Vc,const bool (&failed)[View::N]);

bool IAmTheNewLeader(View& Vc);

void Merge(View& Vc, int myRank);

void WedgeView(View& Vc, sst::SST<DerechoRow<View::N>>& gmsSST, int myRank);

} //namespace derecho


#endif /* VIEW_UTILS_H_ */
