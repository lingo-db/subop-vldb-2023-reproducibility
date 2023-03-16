from numsubop import Array,Builder
import math
def haversine(lat2, lon2):
    miles_constant = 3959.0
    lat1 = 0.70984286
    lon1 = 1.2389197
    dlat = lat2.sub_const(lat1)
    dlon = lon2.sub_const(lon1)
    a = dlat.div_const(2).sin().square().add(
        lat2.cos().mul_const(math.cos(lat1)).mul(dlon.div_const(2).sin().square()))
    c = a.sqrt().arcsin().mul_const(2.0)
    mi = c.mul_const(miles_constant)
    return mi

builder = Builder()
veclen = 1 << 29
size = builder.create_size(veclen)
lat2 = Array(size, builder)
lon2 = Array(size, builder)
builder.fill(lat2, veclen, 0.0698132)
builder.fill(lon2, veclen, 0.0698132)
builder.start_timing()
res = haversine(lat2, lon2)
builder.stop_timing()
builder.return_table(res)

with open('haversine.mlir', 'w') as f:
    print("module{", file=f)
    print("func.func @main() {", file=f)
    print(builder.mlir, file=f)
    print("func.return ", file=f)
    print("}", file=f)
    print("}", file=f)