from numsubop import Array,Builder

def blackscholes(price, strike, t, rate, vol):
    c05 = 3.0
    c10 = 1.5
    invsqrt2 = 0.707

    rsig = rate.add(vol.square().mul_const(c05))
    vol_sqrt = vol.mul(t.sqrt())

    d1 = (price.div(strike).log()).add(rsig.mul(t)).div(vol_sqrt)
    d2 = d1.sub(vol_sqrt)

    # these are numpy arrays, so use scipy's erf function. scipy's ufuncs also
    # get routed through the common ufunc routing mechanism, so these work just
    # fine on weld arrays.
    d1 =d1.mul_const(invsqrt2).erf().mul_const(c05).add_const(c05)
    d2 =d2.mul_const(invsqrt2).erf().mul_const(c05).add_const(c05)

    e_rt = rate.sub_const_rev(0.0).mul(t).exp()

    call = price.mul(d1).sub(e_rt.mul(strike).mul(d2))
    put = e_rt.mul(strike).mul(d2.sub_const_rev(c10)).sub(price.mul(d1.sub_const_rev(c10)))
    return call, put

builder = Builder()
veclen = 1 << 27
size = builder.create_size(veclen)
price = Array(size, builder)
builder.fill(price, veclen, 4.0)
strike = Array(size, builder)
builder.fill(strike, veclen, 4.0)
t = Array(size, builder)
builder.fill(t, veclen, 4.0)
rate = Array(size, builder)
builder.fill(rate, veclen, 4.0)
vol = Array(size, builder)
builder.fill(vol, veclen, 4.0)

builder.start_timing()
call, put = blackscholes(price, strike, t, rate, vol)
builder.stop_timing()
builder.return_table(call, 0)
builder.return_table(put, 1)

with open('blackscholes.mlir', 'w') as f:
    print("module{", file=f)
    print("func.func @main() {", file=f)
    print(builder.mlir, file=f)
    print("func.return ", file=f)
    print("}", file=f)
    print("}", file=f)