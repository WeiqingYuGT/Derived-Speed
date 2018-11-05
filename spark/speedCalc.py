##----------------------------------------------
## Version 0.00
## Remove cached points then compute speed between
## current and the next request.
## 
## Version 1.02
## Add lower bound for time difference for speed 
## computation
## 
## Version 1.05 
## Add validation step
##
##----------------------------------------------


from math import acos, sin, cos, sqrt, atan2, radians
from pyspark.sql import Row
from datetime import datetime
import json

class SpD:
    def __init__(self, upper, lower, thres):
        self.upper = float(upper)
        self.lower = float(lower)
        self.thres = float(thres)

    def _norm(self, vec):
        return sum(x**2 for x in vec)**0.5

    def _angle(self,v1, v2):
        n1, n2 = self._norm(v1), self._norm(v2)
        if (n1<0.00001 and n2<0.00001) or n1==0 or n2==0:
            return -1.0
        else:
            v1, v2 = [x*1000 for x in v1], [x*1000 for x in v2]
            val = (v1[0]*v2[0]+v1[1]*v2[1])/n1/n2/1000/1000
            val = max(val,-1.0)
            val = min(val,1.0)
            return acos(val)

    def _push_interval(self,p1,p2,p3,x):
        while x[p1].sec<x[p2].sec-self.thres:
            p1+=1
        while x[p3].sec<=x[p2].sec+self.thres:
            if p3==len(x)-1:
                break
            p3+=1
        if p3==len(x)-1 and x[p3].sec<=x[p2].sec+self.thres:
            pass
        else:
            p3-=1
        return p1, p3

    def _valSpd(self,dat,dspd):
        rec, ans, ans1 = {0:-1.0}, [-1.0], [-1.0]
        for p2 in range(1,len(dat)-1):
            #if dspd[p2]>20 and any(self._proxmodes(x[2])>0 for x in dat[p2][1]):
            p1 = p2 if dat[p2-1].sec<dat[p2].sec-self.thres else p2-1
            p3 = p2 if dat[p2+1].sec>dat[p2].sec+self.thres else p2+1
            if p1!=p2 and p3!=p2:
                ang = self._angle([dat[p1].lat-dat[p2].lat,dat[p1].long-dat[p2].long],\
                                    [dat[p3].lat-dat[p2].lat,dat[p3].long-dat[p2].long])
                rec[p2] = ang
                ans1.append(ang)
            else:
                rec[p2] = -1.0
                ans1.append(-1.0)
        ans1.append(-1.0)
        rec[p2+1] = -1.0
        p1, p3 = 0, 1
        for p2 in range(1, len(dat)-1):
            p1, p3 = self._push_interval(p1, p2, p3, dat)
            if p1!=p2 and p3!=p2:
                tempres = []
                for i in range(p1, p3):
                    tempres.append(rec[i])
                tempres = [x for x in tempres if x>=0]
                if len(tempres)>0:
                    ans.append(float(sum(tempres)/len(tempres)))
                else:
                    ans.append(-1.0)
            else:
                ans.append(-1.0)
        ans.append(-1.0)
        return ans, ans1
                
    def _decimal_place(self, number):
        return decimal.Decimal(str(number)).as_tuple().exponent

    def _ll_dist(self, l1, l2):
        R = 6373.0
        lat1, lon1, lat2, lon2 = radians(round(l1[0],5)), radians(round(l1[1],5)), radians(round(l2[0],5)), radians(round(l2[1],5))
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
        c = 2 * atan2(sqrt(a), sqrt(1 - a))
        return R * c * 0.621371

    def _avgSpeed1(self,sp1,sp2,t1,t2):
        if sp1==-1 and sp2==-1:
            return -1.0, -1.0
        elif sp1==-1:
            return sp2,t2
        elif sp2==-1:
            return sp1,t1
        else:
            return (sp1+sp2)/2,(t1+t2)/2

    def _avgSpeed(self,sp1,sp2):
        if sp1==-1 and sp2==-1:
            return -1.0
        elif sp1==-1:
            return sp2
        elif sp2==-1:
            return sp1
        else:
            return (sp1+sp2)/2

    def _dumpJSON(self,angle,dspd):
        return json.dumps({"angle":angle,"dspd":dspd})
    
    def _addZero(self,n):
        return '0'+str(n) if n<10 else str(n)
    ## more delta t and delta d
    def getSpeed(self,datum):
        uid, dat = datum[0], datum[1]
        if len(dat)<4:
            spd, ans, ans1 = [-1.0]*len(dat), [-1.0]*len(dat), [-1.0]*len(dat)
        else:
            spd_thr = 100
            dat = sorted(dat, key = lambda x: x[0])
            t1, ll1 = dat[1][0], dat[1][2:4]

            td = (dat[1][0]-dat[0][0])*1.0/60
            dist = self._ll_dist(dat[0][2:4],dat[1][2:4])
            if td<self.upper:
                spd = [dist*1.0/td*60 if dist*1.0/td*60<spd_thr else -1.0]
            else:
                spd = [-1.0]
            lsp, dt1 = spd[-1], td
    
            for d in dat[2:]:
                td = (d[0]-t1)*1.0/60
                dist = self._ll_dist(ll1,d[2:4])
                if td<self.upper:
                    nspd, ntt= self._avgSpeed1(lsp,dist*1.0/td*60,dt1,td)
                    spd.append(nspd if nspd<spd_thr else -1.0)
                    lsp = dist*1.0/td*60 
                    lsp = lsp if lsp<spd_thr else -1.0
                else:
                    nspd, ntt = self._avgSpeed1(-1.0,lsp,td,dt1)
                    spd.append(nspd if nspd<spd_thr else -1.0)
                    lsp = -1.0
                dt1 = td
                t1, ll1 = d[0], d[2:4]
        
            if td<self.upper:
                nspd, ntt= dist*1.0/td*60, td
                spd.append(nspd if nspd<spd_thr else -1.0)
            else:
                spd.append(-1.0)

            ans, ans1 = self._valSpd(dat,spd)

        res = []
        row = Row('request_id','logType','y','m','d','h','stage','isFilled','confScore','derived_speed')
        for d in zip(dat,spd,ans1):
            for v in d[0][1]:
                rl = [v.request_id, v.logType, v.y, self._addZero(v.m),\
                    self._addZero(v.d), self._addZero(v.h),v.stage,v.isFilled,v.confScore]+\
                    [self._dumpJSON(float(d[2]),float(d[1]))]
                res.append(row(*rl))
        return res

