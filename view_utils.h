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

template <typename handlersType>
bool IAmTheNewLeader(View<handlersType>& Vc);

template <typename handlersType>
void merge_changes(View<handlersType>& Vc);

template <typename handlersType>
void wedge_view(View<handlersType>& Vc);

}  // namespace derecho

#include "view_utils_impl.h"

#endif /* VIEW_UTILS_H_ */
