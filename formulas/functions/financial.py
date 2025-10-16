#!/usr/bin/env python
# -*- coding: UTF-8 -*-
#
# Copyright 2016-2025 European Commission (JRC);
# Licensed under the EUPL (the 'Licence');
# You may not use this work except in compliance with the Licence.
# You may obtain a copy of the Licence at: http://ec.europa.eu/idabc/eupl

"""
Python equivalents of financial Excel functions.
"""
import itertools
import functools
import numpy as np
from calendar import monthrange, leapdays, isleap
from . import (
    get_error, Error, wrap_func, raise_errors, text2num, flatten, Array,
    replace_empty, _text2num, wrap_ufunc, convert2float, _get_single_args,
    is_number, XlError, _convert2float
)
from ..errors import FoundError
from .date import year_days, day_count, xday, xdate, _int2date
import datetime
from dateutil.relativedelta import relativedelta
from collections import deque

FUNCTIONS = {}


def xdate2date(*date):
    if date == (1900, 1, 0):
        return datetime.date(1899, 12, 31)
    return datetime.date(*date)


def parse_date(date):
    if isinstance(date, bool):
        raise FoundError(err=Error.errors['#VALUE!'])
    return xday(date, slice(0, 3))


def _xcoup_validate(settlement, maturity, frequency, basis=0):
    if settlement >= maturity or frequency not in (1, 2, 4) or not (
            0 <= basis <= 4
    ):
        raise FoundError(err=Error.errors['#NUM!'])
    return True


def _xcoup(settlement, maturity, frequency, basis=0):
    _xcoup_validate(settlement, maturity, frequency, basis)
    if monthrange(maturity[0], maturity[1])[1] == maturity[2]:
        dt = relativedelta(months=12 // frequency, day=31)
    else:
        dt = relativedelta(months=12 // frequency)
    d = xdate2date(*maturity)
    settlement = xdate2date(*settlement)

    while d > settlement:
        yield _to_date(d)
        d = d - dt
    yield _to_date(d)


def _to_date(x):
    return max((x.year, x.month, x.day), (1900, 1, 0))


def _build_coupon_schedule(
        issue, first_interest, settlement, frequency, calc_method):
    if monthrange(first_interest[0], first_interest[1])[1] == first_interest[2]:
        dt = relativedelta(months=12 // frequency, day=31)
    else:
        dt = relativedelta(months=12 // frequency)

    dates = [first_interest]
    d = xdate2date(*first_interest)
    sett = xdate2date(*settlement)
    v = d
    if calc_method:
        i = xdate2date(*issue)
    else:
        i = min(d - dt, sett)
    while v > i:
        v = v - dt
        dates.insert(0, _to_date(v))
    dates.insert(0, _to_date(v))

    # go forwards
    while d < sett:
        nxt = d + dt
        dates.append(_to_date(nxt))
        d = nxt
    return dates


def parse_basis(basis, func=int):
    if isinstance(basis, bool):
        raise FoundError(err=Error.errors['#VALUE!'])
    return func(basis)


def xaccrint(
        issue, first_interest, settlement, rate, par, frequency, basis=0,
        calc_method=1):
    frequency = int(frequency)
    basis = int(basis)
    _xcoup_validate(issue, settlement, frequency, basis)

    if rate <= 0 or par <= 0:
        raise FoundError(err=Error.errors['#NUM!'])

    periods = list(itertools.pairwise(_build_coupon_schedule(
        issue, first_interest, settlement, frequency, calc_method
    )))

    total = 0.0
    if basis in (0, 2, 4):  # US 30/360 & Actual/360 & Eurobond 30/360
        Di = 360 / frequency
    elif basis == 3:  # Actual/365
        Di = 365 / frequency
    if not calc_method:
        ncd, pcd = deque(_xcoup(
            issue, first_interest, frequency, basis
        ), maxlen=2)
        if pcd < issue < ncd:
            periods.insert(0, (pcd, ncd))

    for start, end in periods:
        s = max(start, issue)
        e = min(end, settlement)
        if e > s:
            if basis == 1:
                Di = day_count(start, end, basis, exact=True)

            total += day_count(s, e, basis=basis, exact=True) / Di
    return float(total * par * rate / frequency)


FUNCTIONS['ACCRINT'] = wrap_ufunc(
    xaccrint,
    input_parser=lambda issue, first_interest, settlement, rate, par, frequency,
                        basis=0, calc_method=1: (
        parse_date(issue),
        parse_date(first_interest),
        parse_date(settlement),
        parse_basis(rate, float),
        parse_basis(par, float),
        parse_basis(frequency, float),
        parse_basis(basis),
        calc_method
    ),
    args_parser=lambda *a: map(replace_empty, a)
)


def xaccrintm(issue, settlement, rate, par, basis=0):
    _xcoup_validate(issue, settlement, 1, basis)
    if isinstance(rate, bool) or isinstance(par, bool):
        raise FoundError(err=Error.errors['#VALUE!'])
    rate = float(rate)
    par = float(par)
    if rate <= 0 or par <= 0:
        raise FoundError(err=Error.errors['#NUM!'])
    dates = issue, settlement
    total = day_count(*dates, basis=basis, exact=True)
    total /= year_days(*dates, basis=basis)
    return float(total * par * rate)


FUNCTIONS['ACCRINTM'] = wrap_ufunc(
    xaccrintm,
    input_parser=lambda issue, settlement, rate, par, basis=0: (
        parse_date(issue),
        parse_date(settlement),
        parse_basis(rate, float),
        parse_basis(par, float),
        parse_basis(basis)
    ),
    args_parser=lambda *a: map(replace_empty, a)
)


def xcoupnum(settlement, maturity, frequency, basis=0, *, coupons=()):
    n = -1
    for ncd in coupons or _xcoup(settlement, maturity, frequency, basis):
        n += 1
    return n


def xcoupncd(settlement, maturity, frequency, basis=0, *, coupons=()):
    ncd = deque(coupons or _xcoup(
        settlement, maturity, frequency, basis
    ), maxlen=2)[0]
    return xdate(*ncd)


def xcouppcd(settlement, maturity, frequency, basis=0, *, coupons=()):
    pcd = deque(coupons or _xcoup(
        settlement, maturity, frequency, basis
    ), maxlen=2)[1]
    return xdate(*pcd)


def xcoupdays(settlement, maturity, frequency, basis=0, *, coupons=()):
    if basis == 1:  # Actual/Actual
        ncd, pcd = deque(coupons or _xcoup(
            settlement, maturity, frequency, basis
        ), maxlen=2)
        return day_count(pcd, ncd, basis, exact=True)
    _xcoup_validate(settlement, maturity, frequency, basis)
    if basis in (0, 2, 4):  # 30/360 US, Actual/360, 30E/360
        return 360.0 / frequency
    if basis == 3:  # Actual/365
        return 365.0 / frequency


def xcoupdaybs(settlement, maturity, frequency, basis=0, *, coupons=()):
    pcd = deque(coupons or _xcoup(
        settlement, maturity, frequency, basis
    ), maxlen=2)[1]
    return day_count(pcd, settlement, basis, exact=True)


def xcoupdaysnc(settlement, maturity, frequency, basis=0, *, coupons=()):
    ncd = deque(coupons or _xcoup(
        settlement, maturity, frequency, basis
    ), maxlen=2)[0]
    return day_count(settlement, ncd, basis or 4)


kw_coup = {
    'input_parser': lambda settlement, maturity, frequency, basis=0: (
        parse_date(settlement), parse_date(maturity),
        parse_basis(frequency), parse_basis(basis)
    ), 'check_error': get_error, 'args_parser': lambda *a: map(replace_empty, a)
}
FUNCTIONS['COUPNUM'] = wrap_ufunc(xcoupnum, **kw_coup)
FUNCTIONS['COUPNCD'] = wrap_ufunc(xcoupncd, **kw_coup)
FUNCTIONS['COUPPCD'] = wrap_ufunc(xcouppcd, **kw_coup)
FUNCTIONS['COUPDAYS'] = wrap_ufunc(xcoupdays, **kw_coup)
FUNCTIONS['COUPDAYBS'] = wrap_ufunc(xcoupdaybs, **kw_coup)
FUNCTIONS['COUPDAYSNC'] = wrap_ufunc(xcoupdaysnc, **kw_coup)


def xduration(
        settlement, maturity, coupon, yld, frequency, basis=0, *, face=100.0,
        modified=False
):
    coupons = tuple(_xcoup(settlement, maturity, frequency, basis))
    DSC = xcoupdaysnc(
        settlement, maturity, frequency, basis=basis, coupons=coupons
    )
    E = xcoupdays(
        settlement, maturity, frequency, basis=basis, coupons=coupons
    )
    N = xcoupnum(
        settlement, maturity, frequency, basis=basis, coupons=coupons
    )
    df = 1.0 + yld / frequency

    e = min(1.0, DSC / E) + np.arange(N, dtype=float)
    disc = df ** e

    cf = np.full(N, coupon * face / frequency, dtype=float)
    cf[-1] += face

    pv = cf / disc
    price = pv.sum()
    if price == 0:
        raise FoundError(err=Error.errors["#DIV/0"])

    t_years = e / frequency
    D_mac = (t_years * pv).sum() / price
    if modified:
        D_mac /= df
    return float(D_mac)


def xpduration(rate, pv, fv):
    if np.any(rate <= 0) or np.any(pv <= 0) or np.any(fv <= 0):
        raise FoundError(err=Error.errors["#NUM!"])
    return np.log(fv / pv) / np.log1p(rate)


def xrri(rate, pv, fv):
    if np.any(rate <= 0) or np.any(pv <= 0) or np.any(fv <= 0):
        raise FoundError(err=Error.errors["#NUM!"])
    return (fv / pv) ** (1 / rate) - 1


kw_duration = {
    'input_parser': lambda settlement, maturity, coupon, yld, frequency,
                           basis=0: (
        parse_date(settlement),
        parse_date(maturity),
        parse_basis(coupon, float),
        parse_basis(yld, float),
        parse_basis(frequency),
        parse_basis(basis)
    ),
    'check_error': get_error, 'args_parser': lambda *a: map(replace_empty, a)
}

FUNCTIONS['DURATION'] = wrap_ufunc(xduration, **kw_duration)
FUNCTIONS['MDURATION'] = wrap_ufunc(
    functools.partial(xduration, modified=True), **kw_duration
)
FUNCTIONS['_XLFN.PDURATION'] = FUNCTIONS['PDURATION'] = wrap_ufunc(xpduration)
FUNCTIONS['_XLFN.RRI'] = FUNCTIONS['RRI'] = wrap_ufunc(xrri)


def xeffect(nominal_rate, npery):
    nominal_rate = replace_empty(np.atleast_2d(nominal_rate))
    npery = replace_empty(np.atleast_2d(npery))
    if not (nominal_rate.size == npery.size == 1):
        raise FoundError(err=Error.errors['#VALUE!'])
    nominal_rate = parse_basis(nominal_rate.item(), _convert2float)
    npery = int(parse_basis(npery.item(), _convert2float))
    if nominal_rate <= 0 or npery < 1:
        raise FoundError(err=Error.errors['#NUM!'])

    return (1.0 + nominal_rate / npery) ** npery - 1.0


def xnominal(effect_rate, npery):
    effect_rate = replace_empty(np.atleast_2d(effect_rate))
    npery = replace_empty(np.atleast_2d(npery))
    if not (effect_rate.size == npery.size == 1):
        raise FoundError(err=Error.errors['#VALUE!'])
    effect_rate = parse_basis(effect_rate.item(), _convert2float)
    npery = int(parse_basis(npery.item(), _convert2float))
    if effect_rate <= 0 or npery < 1:
        raise FoundError(err=Error.errors['#NUM!'])
    return ((effect_rate + 1.0) ** (1.0 / npery) - 1.0) * npery


FUNCTIONS['EFFECT'] = wrap_func(xeffect)
FUNCTIONS['NOMINAL'] = wrap_func(xnominal)


def args_parser_intrate(settlement, maturity, investment, redemption, basis=0):
    settlement = parse_date(replace_empty(np.asarray(settlement).item()))
    maturity = parse_date(replace_empty(np.asarray(maturity).item()))

    investment = replace_empty(np.asarray(investment).item())
    redemption = replace_empty(np.asarray(redemption).item())
    if isinstance(investment, bool) or isinstance(redemption, bool):
        raise FoundError(err=Error.errors['#VALUE!'])
    investment = _convert2float(investment)
    redemption = _convert2float(redemption)
    if settlement >= maturity:
        raise FoundError(err=Error.errors["#NUM!"])
    if investment <= 0 or redemption <= 0:
        raise FoundError(err=Error.errors["#NUM!"])
    dates = settlement, maturity
    return (redemption - investment) / investment, dates, basis


def xintrate(num, dates, basis=0):
    yf = day_count(*dates, basis=basis) / year_days(*dates, basis=basis)
    return num / yf


FUNCTIONS['INTRATE'] = wrap_ufunc(
    xintrate,
    input_parser=lambda num, dates, basis: (num, dates, parse_basis(basis)),
    args_parser=args_parser_intrate,
    excluded={0, 1}
)


def args_parser_received(settlement, maturity, investment, discount, basis=0):
    settlement = parse_date(replace_empty(np.asarray(settlement).item()))
    maturity = parse_date(replace_empty(np.asarray(maturity).item()))

    investment = replace_empty(np.asarray(investment).item())
    discount = replace_empty(np.asarray(discount).item())
    if isinstance(investment, bool) or isinstance(discount, bool):
        raise FoundError(err=Error.errors['#VALUE!'])
    investment = _convert2float(investment)
    discount = _convert2float(discount)
    if settlement >= maturity:
        raise FoundError(err=Error.errors["#NUM!"])
    if investment <= 0 or discount <= 0:
        raise FoundError(err=Error.errors["#NUM!"])
    dates = settlement, maturity
    return investment, discount, dates, basis


def xreceived(investment, discount, dates, basis=0):
    yf = day_count(*dates, basis=basis) / year_days(*dates, basis=basis)
    return investment / (1 - discount * yf)


FUNCTIONS['RECEIVED'] = wrap_ufunc(
    xreceived,
    input_parser=lambda investment, discount, dates, basis: (
        investment, discount, dates, parse_basis(basis)
    ),
    args_parser=args_parser_received,
    excluded={0, 1, 2}
)


def args_parser_disc(settlement, maturity, pr, redemption, basis=0):
    settlement = parse_date(replace_empty(np.asarray(settlement).item()))
    maturity = parse_date(replace_empty(np.asarray(maturity).item()))

    pr = replace_empty(np.asarray(pr).item())
    redemption = replace_empty(np.asarray(redemption).item())
    if isinstance(pr, bool) or isinstance(redemption, bool):
        raise FoundError(err=Error.errors['#VALUE!'])
    pr = _convert2float(pr)
    redemption = _convert2float(redemption)
    if settlement >= maturity:
        raise FoundError(err=Error.errors["#NUM!"])
    if redemption <= 0 or pr <= 0:
        raise FoundError(err=Error.errors["#NUM!"])
    dates = settlement, maturity
    return (redemption - pr) / redemption, dates, basis


def xdisc(num, dates, basis=0):
    yf = day_count(*dates, basis=basis) / year_days(*dates, basis=basis)
    return num / yf


FUNCTIONS['DISC'] = wrap_ufunc(
    xdisc,
    input_parser=lambda num, dates, basis: (num, dates, parse_basis(basis)),
    args_parser=args_parser_disc,
    excluded={0, 1}
)


def xdb(cost, salvage, life, period, month=12):
    if cost > 0 and salvage >= 0 and life > 0 and period > 0 and 0 < month < 13:
        rate = round(1 - (salvage / cost) ** (1 / life), 3)
        period = int(period)
        month = int(month)
        life = int(life)
        if period > (life if month == 12 else (life + 1)):
            raise FoundError(err=Error.errors["#NUM!"])

        depk = cost * rate * (month / 12.0)
        dep_cum = 0

        for k in range(2, min(period, life) + 1):
            dep_cum += depk
            base = cost - dep_cum
            depk = base * rate
        if month != 12 and period == (life + 1):
            dep_cum += depk
            base = cost - dep_cum
            depk = base * rate * ((12 - month) / 12.0)
        return depk

    raise FoundError(err=Error.errors["#NUM!"])


FUNCTIONS['DB'] = wrap_ufunc(xdb, input_parser=convert2float)


def xddb(cost, salvage, life, period, factor=2):
    if cost >= 0 and salvage >= 0 and salvage <= cost and 1 <= period <= life and factor > 0:
        p0 = float(period) - 1.0  # Start semi-period.
        p1 = float(period)  # End semi-period.
        rate = factor / life
        if rate >= 1:
            new_value = 0
            old_value = cost if period == 1 else 0
        else:
            base = 1.0 - rate
            old_value = cost * (base ** (period - 1))
            new_value = cost * (base ** period)
        return max(old_value - max(new_value, salvage), 0)

    raise FoundError(err=Error.errors["#NUM!"])


FUNCTIONS['DDB'] = wrap_ufunc(xddb, input_parser=convert2float)


def xdollarde(fractional, denominator):
    fractional = parse_basis(fractional, _convert2float)
    denominator = int(parse_basis(denominator, _convert2float))
    if denominator > 0:
        int_part = np.trunc(fractional)
        frac_part = fractional - int_part
        scale10 = 10 ** np.ceil(np.log10(denominator))
        return int_part + (frac_part * scale10) / denominator
    elif denominator == 0:
        raise FoundError(err=Error.errors["#DIV/0!"])
    raise FoundError(err=Error.errors["#NUM!"])


def xdollarfr(decimal_dollar, fraction):
    decimal_dollar = parse_basis(decimal_dollar, _convert2float)
    fraction = int(parse_basis(fraction, _convert2float))
    if fraction > 0:
        result = int(decimal_dollar)
        result += (decimal_dollar % 1) * np.pow(10, -np.ceil(
            np.log(fraction) / np.log(10)
        )) * fraction

        return result

    raise FoundError(err=Error.errors["#NUM!"])


FUNCTIONS['DOLLARDE'] = wrap_ufunc(xdollarde, input_parser=lambda *a: a)
FUNCTIONS['DOLLARFR'] = wrap_ufunc(xdollarfr, input_parser=lambda *a: a)


def _xnpv(values, dates=None, min_date=0):
    err = get_error(dates, values)
    if not err and \
            any(isinstance(v, bool) for v in flatten((dates, values), None)):
        err = Error.errors['#VALUE!']
    if err:
        return lambda rate: err, None

    values, dates = tuple(map(replace_empty, (values, dates)))
    _ = lambda x: np.array(text2num(replace_empty(x)), float).ravel()
    if dates is None:
        values = _(values)
        t = np.arange(1, values.shape[0] + 1)
    else:
        dates = np.floor(_(dates))
        i = np.argsort(dates)
        values, dates = _(values)[i], dates[i]
        if len(values) != len(dates) or (dates <= min_date).any() or \
                (dates >= 2958466).any():
            return lambda rate: Error.errors['#NUM!'], None
        t = (dates - dates[0]) / 365

    def func(rate):
        return (values / np.power(1 + rate, t)).sum()

    t1, tv = t + 1, -t * values

    def dfunc(rate):
        return (tv / np.power(1 + rate, t1)).sum()

    return func, dfunc


def xnpv(rate, values, dates=None):
    with np.errstate(divide='ignore', invalid='ignore'):
        func = _xnpv(values, dates)[0]

        def _(r):
            e = isinstance(r, str) and Error.errors['#VALUE!']
            return get_error(r, e) or func(r)

        rate = text2num(replace_empty(rate))
        return np.vectorize(_, otypes=[object])(rate).view(Array)


def xxnpv(rate, values, dates):
    rate = np.asarray(rate)
    if rate.size > 1:
        return Error.errors['#VALUE!']
    raise_errors(rate)
    rate = _text2num(replace_empty(rate).ravel()[0])
    if isinstance(rate, (bool, str)):
        return Error.errors['#VALUE!']
    if rate <= 0:
        return Error.errors['#NUM!']

    return xnpv(rate, values, dates)


FUNCTIONS['NPV'] = wrap_func(lambda r, v, *a: xnpv(r, tuple(flatten((v, a)))))
FUNCTIONS['XNPV'] = wrap_func(xxnpv)


def _npf(func, *args, freturn=lambda x: x, **kwargs):
    import numpy_financial as npf
    r = getattr(npf, func)(*args, **kwargs)
    return freturn(r if getattr(r, 'shape', True) else r.ravel()[0])


FUNCTIONS['FV'] = wrap_ufunc(
    functools.partial(_npf, 'fv'),
    check_error=lambda *args: None,
    input_parser=lambda rate, nper, pmt, pv=0, type=0: convert2float(
        rate, nper, pmt, pv, type
    )
)


def args_parser_fvschedule(principal, schedule):
    schedule = tuple(flatten(schedule, None, drop_empty=True))
    for v in schedule:
        if not isinstance(v, (int, float)) or isinstance(v, bool):
            return principal, get_error(v) or Error.errors['#VALUE!']
    return replace_empty(principal), np.prod(1 + np.array(schedule))


def xfvschedule(principal, prod):
    principal = parse_basis(principal, _convert2float)
    raise_errors(prod)
    return principal * prod


FUNCTIONS['FVSCHEDULE'] = wrap_ufunc(
    xfvschedule, input_parser=lambda *a: a,
    check_error=lambda *args: None,
    args_parser=args_parser_fvschedule,
    excluded={1}
)


def xispmt(rate, per, nper, pv):
    if not nper:
        raise FoundError(err=Error.errors['#DIV/0!'])
    return pv * rate * (per / nper - 1)


FUNCTIONS['ISPMT'] = wrap_ufunc(xispmt, input_parser=convert2float)


def xsln(cost, salvage, life):
    if not life:
        raise FoundError(err=Error.errors['#DIV/0!'])
    return (cost - salvage) / life


def xsyd(cost, salvage, life, per):
    if per <= 0 or per > life:
        raise FoundError(err=Error.errors['#NUM!'])
    return ((cost - salvage) * (life - per + 1) * 2) / (life * (life + 1))


FUNCTIONS['SLN'] = wrap_ufunc(xsln, input_parser=convert2float)
FUNCTIONS['SYD'] = wrap_ufunc(xsyd, input_parser=convert2float)


def xcumipmt(rate, nper, pv, start_period, end_period, type, *,
             func=functools.partial(_npf, 'ipmt')):
    args = rate, nper, pv, start_period, end_period, type
    args = tuple(map(_text2num, _get_single_args(*map(replace_empty, args))))
    raise_errors(*args)
    if any(not isinstance(v, (float, int)) for v in args):
        return Error.errors['#VALUE!']
    rate, nper, pv, start_period, end_period, type = args
    if rate <= 0 or nper <= 0 or pv <= 0 or start_period < 1 or \
            end_period < 1 or start_period > end_period or not type in (0, 1) \
            or nper < start_period or end_period > nper:
        return Error.errors['#NUM!']
    per = list(range(int(start_period), int(end_period + 1)))
    res = func(rate, per, nper, pv, fv=0, when=type)
    return res[res < 0].sum()


FUNCTIONS['CUMIPMT'] = wrap_func(xcumipmt)
FUNCTIONS['CUMPRINC'] = wrap_func(
    functools.partial(xcumipmt, func=functools.partial(_npf, 'ppmt')))

_kw = {'input_parser': convert2float}
FUNCTIONS['PV'] = wrap_ufunc(functools.partial(_npf, 'pv'), **_kw)
FUNCTIONS['IPMT'] = wrap_ufunc(functools.partial(
    _npf, 'ipmt', freturn=lambda x: x > 0 and Error.errors['#NUM!'] or x,
), **_kw)
FUNCTIONS['PMT'] = wrap_ufunc(functools.partial(_npf, 'pmt'), **_kw)


def xppmt(rate, per, nper, pv, fv=0, type=0):
    import numpy_financial as npf
    if per < 1 or per >= nper + 1:
        return Error.errors['#NUM!']
    return npf.ppmt(rate, per, nper, pv, fv=fv, when=type)


FUNCTIONS['PPMT'] = wrap_ufunc(xppmt, **_kw)


def xrate(nper, pmt, pv, fv=0, type=0, guess=0.1):
    with np.errstate(over='ignore'):
        import numpy_financial as npf
        return npf.rate(
            nper, pmt, pv, fv=fv, when=type, guess=guess, maxiter=1000
        )


FUNCTIONS['RATE'] = wrap_ufunc(xrate, **_kw)


def xnper(rate, pmt, pv, fv=0, type=0):
    import numpy_financial as npf
    r = npf.nper(rate, pmt, pv, fv=fv, when=type)
    if rate == 0:
        r = np.sign(npf.nper(0.00000001, pmt, pv, fv=fv, when=type)) * np.abs(r)
    return r


FUNCTIONS['NPER'] = wrap_ufunc(xnper, **_kw)


def xirr(values, guess=0.1):
    with np.errstate(divide='ignore', invalid='ignore'):
        import numpy_financial as npf
        res = npf.irr(tuple(flatten(text2num(replace_empty(values)).ravel())))
        res = (not np.isfinite(res)) and Error.errors['#NUM!'] or res

        def _(g):
            e = isinstance(g, str) and Error.errors['#VALUE!']
            return get_error(g, e) or res

        guess = text2num(replace_empty(guess))
        return np.vectorize(_, otypes=[object])(guess).view(Array)


FUNCTIONS['IRR'] = wrap_func(xirr)


def mirr_args_parser(values, finance_rate, reinvest_rate):
    values = tuple(flatten(
        values.ravel(), check=lambda v: isinstance(v, XlError) or (
                not isinstance(v, str) and is_number(v)
        )
    ))

    return (values, replace_empty(finance_rate), replace_empty(reinvest_rate))


def xmirr(values, finance_rate, reinvest_rate):
    raise_errors(finance_rate, reinvest_rate, values)
    res = _npf('mirr', values, finance_rate, reinvest_rate)
    if np.isnan(res):
        raise FoundError(err=Error.errors['#DIV/0!'])
    return res


FUNCTIONS['_XLFN.MIRR'] = FUNCTIONS['MIRR'] = wrap_ufunc(
    xmirr,
    args_parser=mirr_args_parser,
    check_error=lambda *a: None,
    input_parser=lambda values, finance_rate, reinvest_rate: (
            (values,) + tuple(convert2float(finance_rate, reinvest_rate))
    ), excluded={0}
)


def _newton(f, df, x, tol=.0000001):
    xmin = tol - 1
    with np.errstate(divide='ignore', invalid='ignore'):
        for _ in range(100):
            dx = f(x) / df(x)
            if not np.isfinite(dx):
                break
            if abs(dx) <= tol:
                return x
            x = max(xmin, x - dx)
    return Error.errors['#NUM!']


def xxirr(values, dates, x=0.1):
    x = np.asarray(x, object)
    if x.size > 1:
        return Error.errors['#VALUE!']
    raise_errors(x)
    x = _text2num(replace_empty(x).ravel()[0])
    if isinstance(x, (bool, str)):
        return Error.errors['#VALUE!']
    if x < 0:
        return Error.errors['#NUM!']
    f, df = _xnpv(values, dates, min_date=-1)
    if df is None:
        return f(x)
    return _newton(f, df, x)


FUNCTIONS['XIRR'] = wrap_func(xxirr)
