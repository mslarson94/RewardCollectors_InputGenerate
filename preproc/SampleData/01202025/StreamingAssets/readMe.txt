streaming asset files are grouped by day (plus the dummy set for troubleshooting) be sure to move copies of the files you need into the StreamingAssets directory itself & rename to omit the "_X" portion of the file name 

####################################################
################## IP Addresses ####################
####################################################

###################    AN    #######################

if whichDevice_AN == 'ML2A':
        if whichWifi == 'SuthanaLabResearch':
                ipAddress_AN = '192.168.50.109'
        elif whichWifi == 'SuthanaLab': 
                ipAddresss_AN = '192.168.1.84'
elif whichDevice_AN == 'ML2F':
        if whichWifi == "SuthanaLabResearch":
                ipAddress_AN = '192.168.50.XX'
        elif whichWifi == 'SuthanaLab':
                ipAddresss_AN = '192.168.1.218'

###################    PO    #######################

if whichDevice_PO == 'ML2D':
        if whichWifi == 'SuthanaLabResearch':
                ipAddress_PO = '192.168.50.127'
        elif whichWifi == 'SuthanaLab': 
                ipAddress_PO = '192.168.1.146'

elif whichDevice_PO == 'ML2B':
        if whichWifi == "SuthanaLabResearch":
                ipAddress_PO = '192.168.50.133'
        elif whichWifi == 'SuthanaLab': 
                ipAddress_PO = '192.168.1.XX'
elif whichDevice_PO == 'ML2C':
        if whichWifi == "SuthanaLabResearch":
                ipAddress_PO = '192.168.50.XX'
        elif whichWifi == 'SuthanaLab': 
                ipAddress_PO = '192.168.50.XX'
