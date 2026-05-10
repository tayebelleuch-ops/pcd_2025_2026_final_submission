from math import pi,sin,cos,tan,arccos,exp,sqrt
def calculate_Ra(j,year,lat):
    phi=lat*pi/180
    if (year%4==0):
        ang=366
    else:
        ang=365
    jang=(2*pi*j)/ang
    dr=1+0.033*cos(jang)
    delta=0.409*sin(jang-1.39)
    ws=arccos(-tan(phi)*tan(delta))
    Ra=(24*60/pi)*0.0820*dr*((ws*sin(phi)*sin(delta)+cos(phi)*cos(delta)*sin(ws)))
    return Ra
def esat(temp):return 0.6108*exp((17.27*temp)/(temp+237.3))   
def calcul_ET0(
    Rs, #from nasapower
    humid_rel_mean,# in %
    temp_mean,# in °C
    temp_min,# in °C
    temp_max,# in °C
    wind2m,# in m/s
    altitude,# in m
    j,# in j
    year,# in year
    lattitude,# in degrees

    ):
    
    es=(esat(temp_max)+esat(temp_min))/2 #in kPa
    ea=es*humid_rel_mean/100 #in kPa
    
    Rso=(0.75+0.000002*altitude)*calculate_Ra(j,year,lattitude)

    RNL=sigma*((((temp_max+273.15)**4)+((temp_min+273.15)**4))/2)*(0.34-0.14*sqrt(ea))*(1.35*(Rs/Rso)-0.35) #in MJ/(m².day)
    RNS=0.77*Rs      #in MJ/(m².day)
    
    
    delta=4098*esat(temp_mean)/((temp_mean+237.3)**2)# in kPa/°C
    press_atm=101.3*(((293-0.0065*altitude)/293)**5.26)# in kPa
    gamma=0.000665*press_atm# in kPa/°C
    res = (
        (0.408*(RNS-RNL)+gamma*(900/(temp_mean+273.15))*((es-ea)*wind2m))       
        /
        (delta+gamma*(1+0.34*wind2m)))
    return res