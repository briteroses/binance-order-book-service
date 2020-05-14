from snapshots import snapshots
from updates import updates_f, updates_b
import copy

def reconstruct(timestampId):
    '''
    Reconstructs the order book at specified timestampId (on the same scale as the update IDs)
     -> Finds the most recent snapshot before timestamp and initial update to be processed
        (Runs backwards reconstruction if first update is erroneous)
     -> Pushes consecutive updates to base snapshot, starting from initial one,
        until the first update that exceeds the timestamp is pushed
        (Pushing an update consists of changing, adding, removing all asks and bids in the update
        as each operation is detailed in design doc)
        (Runs backwards reconstruction if missing update is detected)
    '''
    base_snap_Id = updateId
    u_capture = 0
    while base_snap_Id not in snapshots:
        if base_snap_Id in updates_b:
            u_capture = base_snap_Id
        base_snap_Id -= 1
    if u_capture == 0:
        return snapshots[base_snap_Id]['order_book']
    if updates_b[u_capture]['U'] > base_snap_Id + 1:
        return reconstruct_backwards(timestampId)
    base_snap = copy.deepcopy(snapshots[base_snap_Id])
    while u_capture < timestampId:
        asks, bids = updates_b[u_capture]['a'], updates_b[u_capture]['b']
        for a in asks:
            if a[0] in base_snap['order_book']:
                #REMOVE ASK
                if a[1] == 0:
                    f_pointer = base_snap['order_book'][a[0]][1]
                    b_pointer = base_snap['order_book'][a[0]][2]
                    if f_pointer is not None:
                        base_snap['order_book'][f_pointer][2] = b_pointer
                    if b_pointer is not None:
                        base_snap['order_book'][f_pointer][1] = f_pointer
                    base_snap['order_book'].pop(a[0])
                    if a[0] == base_snap['least_ask']:
                        base_snap['least_ask'] = f_pointer
                #CHANGE EXISTING ASK
                else:
                    base_snap['order_book'][a[0]][0] = a[1]
            else:
                #REMOVE ASK NOT IN BOOK
                if a[1] == 0:
                    continue
                #ADD NEW LEAST ASK
                else if a[0] < base_snap['least_ask']:
                    base_snap['order_book'][base_snap['least_ask']][2] = a[0]
                    base_snap['order_book'][a[0]] = [a[1], base_snap['least_ask'], None]
                    base_snap['least_ask'] = a[0]
                #ADD NEW ASK
                else:
                    f_pointer = base_snap['least_ask']
                    while f_pointer < a[0]:
                        f_pointer = base_snap['order_book'][f_pointer][1]
                    b_pointer = base_snap['order_book'][f_pointer][2]
                    base_snap['order_book'][f_pointer][2] = a[0]
                    base_snap['order_book'][b_pointer][1] = a[0]
                    base_snap['order_book'][a[0]] = [a[1], f_pointer, b_pointer]
        for b in bids:
            if b[0] in base_snap['order_book']:
                #REMOVE BID
                if b[1] == 0:
                    f_pointer = base_snap['order_book'][a[0]][1]
                    b_pointer = base_snap['order_book'][a[0]][2]
                    if f_pointer is not None:
                        base_snap['order_book'][f_pointer][2] = b_pointer
                    if b_pointer is not None:
                        base_snap['order_book'][f_pointer][1] = f_pointer
                    base_snap['order_book'].pop(a[0])
                    if a[0] == base_snap['greatest_bid']:
                        base_snap['greatest_bid'] = f_pointer
                #CHANGE EXISTING BID
                else:
                    base_snap['order_book'][a[0]][0] = a[1]
            else:
                #REMOVE BID NOT IN BOOK
                if a[1] == 0:
                    continue
                #ADD NEW GREATEST BID
                else if a[0] > base_snap['greatest_bid']:
                    base_snap['order_book'][base_snap['greatest_bid']][2] = a[0]
                    base_snap['order_book'][a[0]] = [a[1], base_snap['greatest_bid'], None]
                    base_snap['greatest_bid'] = a[0]
                #ADD NEW ASK
                else:
                    f_pointer = base_snap['greatest_bid']
                    while f_pointer > a[0]:
                        f_pointer = base_snap['order_book'][f_pointer][1]
                    b_pointer = base_snap['order_book'][f_pointer][2]
                    base_snap['order_book'][f_pointer][2] = a[0]
                    base_snap['order_book'][b_pointer][1] = a[0]
                    base_snap['order_book'][a[0]] = [a[1], f_pointer, b_pointer]
        if u_capture+1 not in updates_f:
            return reconstruct_backwards(timestampId)
        else:
            u_capture = updates_f[u_capture+1]['u']
    return base_snap

def reconstruct_backwards(timestampId):
    return "Error: dropped update and redundancy not yet implemented"

def avg_price(snapshot, type, quantity):
    '''
    type is either a buy or sell, quantity is number of stocks in transaction
    calculates the average price for a transaction given an order book
    '''
    if type != 'buy' and type != 'sell':
        return "Must specify buy or sell"
    transaction = {}
    counter = 0
    start_point = 0
    if type == 'buy':
        start_point = snapshot['least_ask']
    if type == 'sell':
        start_point = snapshot['greatest_bid']
    while counter < quantity:
        taken = min(snapshot['order_book'][start_point][0], quantity-counter)
        transaction[start_point] = taken
        start_point = snapshot['order_book'][start_point][1]
    weight_sum = 0
    for k, v in transaction.items():
        weight_sum += k*v
    return ans/quantity
