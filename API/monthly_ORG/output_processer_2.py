import xlwings as xl
# AA # of files, BB # of countries
AA = 14
BB = 10
pre_filename = 'API_monthly_ORG_'

#### matchtype
country_list = []
time_list = []
label_list = []
value_list = []
for icon in range(AA):
    filename = pre_filename+str(icon+1)
    wb = xl.Book(filename+'.csv')
    sht = wb.sheets[0]
    timestamp = sht.range('B1').value
    labels = ['NoMatch','PerfectMatch','PossibleMatch','TotalMatch']
    for ii in range(BB):
        if sht.range('F'+str(ii*12+2)).value ==None:
            nomatch = '0'
            totalmatch ='0'
        else:
            nomatch = str(float(sht.range('F'+str(ii*12+2)).value)*100)+'%'
            totalmatch = str(float(1-sht.range('F'+str(ii*12+2)).value)*100)+'%'
        if sht.range('F'+str(ii*12+3)).value==None:
            perfectmatch = '0'
        else:
            perfectmatch = str(float(sht.range('F'+str(ii*12+3)).value)*100)+'%'
        if sht.range('F'+str(ii*12+4)).value==None:
            possiblematch ='0'
        else:
            possiblematch = str(float(sht.range('F'+str(ii*12+4)).value)*100)+'%'
##        if sht.range('F'+str(ii*12+2)).value == None:
##            totalmatch ='0'
##        else:
##            totalmatch = str(float(1-sht.range('F'+str(ii*12+2)).value)*100)+'%'
        #totalmatch = str(float(1-sht.range('F'+str(ii*12+2)).value)*100)+'%'
        #totalcount = int(sht.range('D'+str(ii*12+2)).value)
        country = sht.range('A'+str(ii*12+2)).value
        dummy_list = [nomatch,perfectmatch,possiblematch,totalmatch]
        for jj in range(4):
            country_list.append(country)
            time_list.append(timestamp)
            label_list.append(labels[jj])
            value_list.append(dummy_list[jj])
    wb.close()

wb_res = xl.Book()
wb_res.sheets[0].range('A1').options(transpose=True).value = time_list
wb_res.sheets[0].range('B1').options(transpose=True).value = country_list
wb_res.sheets[0].range('C1').options(transpose=True).value = label_list
wb_res.sheets[0].range('D1').options(transpose=True).value = value_list



## total count
country_list = []
time_list = []
label_list = []
value_list = []
for icon in range(AA):
    filename = pre_filename+str(icon+1)
    wb = xl.Book(filename+'.csv')
    sht = wb.sheets[0]
    timestamp = sht.range('B1').value
    labels = 'TotalCount'
    for ii in range(BB):
        totalcount = int(sht.range('D'+str(ii*12+2)).value)
        country = sht.range('A'+str(ii*12+2)).value
        country_list.append(country)
        time_list.append(timestamp)
        label_list.append(labels)
        value_list.append(totalcount)
    wb.close()
    
wb_res.sheets.add('TotalCount')
wb_res.sheets['TotalCount'].range('A1').options(transpose=True).value = time_list
wb_res.sheets['TotalCount'].range('B1').options(transpose=True).value = country_list
wb_res.sheets['TotalCount'].range('C1').options(transpose=True).value = label_list
wb_res.sheets['TotalCount'].range('D1').options(transpose=True).value = value_list

# fields
country_list = []
time_list = []
label_list = []
value_list = []
for icon in range(AA):
    filename = pre_filename+str(icon+1)
    wb = xl.Book(filename+'.csv')
    sht = wb.sheets[0]
    timestamp = sht.range('B1').value
    labels = ['phones','emails','social_profiles','jobs','educations','addresses','dobs']
    for ii in range(BB):
        country = sht.range('A'+str(ii*12+2)).value
        dummy_list = []
        for kk in range(len(labels)):
            dummy_list.append(1)
        for kk in range(len(labels)):
            dummy_list[kk] = sht.range('R'+str(ii*12+kk+2)).value
        for jj in range(len(labels)):
            country_list.append(country)
            time_list.append(timestamp)
            label_list.append(labels[jj])
            value_list.append(dummy_list[jj])
    wb.close()
    
wb_res.sheets.add('Fields')
wb_res.sheets['Fields'].range('A1').options(transpose=True).value = time_list
wb_res.sheets['Fields'].range('B1').options(transpose=True).value = country_list
wb_res.sheets['Fields'].range('C1').options(transpose=True).value = label_list
wb_res.sheets['Fields'].range('D1').options(transpose=True).value = value_list

# top query
country_list = []
time_list = []
label_list = []
value_list = []
for icon in range(AA):
    filename = pre_filename+str(icon+1)
    wb = xl.Book(filename+'.csv')
    sht = wb.sheets[0]
    timestamp = sht.range('B1').value
    for ii in range(BB):
        country = sht.range('A'+str(ii*12+2)).value
        dummy_list = []
        labels = []
        for kk in range(3,13):
            if sht.range('F'+str(ii*12+2)).value!=None:
                labels.append(sht.range('B'+str(ii*12+kk)).value)
            else:
                labels.append('No query')
                break
        if labels[0]!='No query':
            for kk in range(len(labels)):
                dummy_list.append(1)
            for kk in range(len(labels)):
                dummy_list[kk] = sht.range('C'+str(ii*12+kk+3)).value
            labels.append('Others')
            dummy_list.append(1-sht.range('C'+str(ii*12+13)).value)
        else:
            dummy_list.append('0')
        for jj in range(len(labels)):
            country_list.append(country)
            time_list.append(timestamp)
            label_list.append(labels[jj])
            value_list.append(dummy_list[jj])
    wb.close()
wb_res.sheets.add('TopQuery')
wb_res.sheets['TopQuery'].range('A1').options(transpose=True).value = time_list
wb_res.sheets['TopQuery'].range('B1').options(transpose=True).value = country_list
wb_res.sheets['TopQuery'].range('C1').options(transpose=True).value = label_list
wb_res.sheets['TopQuery'].range('D1').options(transpose=True).value = value_list

wb_res.save('API_monthly_org_overall_1912_2101.xlsx')
