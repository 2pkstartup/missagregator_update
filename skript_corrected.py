# skript pro agregaci čerpání osobních nákladů zaměstnanců kateder ČVUT 
# export z mis.cvut.cz -> Manažerský IS -> Mzdové sestavy -> Rekapitulace čerpání mezd -> ikonka "Export zdrojových dat do MS Excelu"

import pandas as pd
import math
import os
import warnings
import time
import glob


# Record the start time
start_time = time.time()

# definuj začátek a konec pro hledání vstupních souborů
# např. verso_mis_p_rek_cerp_m_table_2007.xls ... verso_mis_p_rek_cerp_m_table_2023.xls (názvy je nutné po exportu z mis.cvut.cz upravit)
year_start = 2007
year_end = 2030

# prefix názvu souboru - "verso_mis_p_rek_cerp_m_table" je výchozí název v mis.cvut.cz
filename_prefix = "verso_mis_p_rek_cerp_m_table_"

# definice čísel TA pro identifikaci, co je čerpáno z katedry (a zbytek se bere, ze je z "projektů")
group_department = [101, 122, 888]

# název výstupního xlsx souboru
excel_file_path = 'output.xlsx'


# ignoruj varování při neoptimalní práci s pandas :D
warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)

# kód:
years = pd.DataFrame()
def read_data_from_xls():
    global years, filename_prefix
    bl = []
    first = True
    for year in range(year_start, year_end+1):                
        filename = filename_prefix+str(year)+".xls"
        if os.path.exists(filename):
            years.at['Y', year] = True
            print(f"The file '{filename}' exists")
            df = pd.read_excel(filename, sheet_name="Sheet1")
            df = pd.DataFrame(df)
            df['ROK'] = df.apply(lambda row: year, axis=1)
            df['OSC'] = df.apply(lambda row: math.floor(row['OSC_PV']), axis=1)
            if first:
                bl = df
                first = False
            else:
                bl = pd.concat([bl, df])
        else:
            years.at['Y', year] = False
    # optimize bl :)
    bl.drop(["'A'","DIVIZE","IDPRAC","KOD","KPL","OSC_PV"], axis=1, inplace=True)
    bl2=bl.groupby(['ROK','MESIC','OSC','PRIJMENI','JMENO','DRUHPOM','IDZAKAZKY','TA','AKCE']).sum().reset_index()
    return bl2

# najdi seznam unikátních zaměstnanců podle OSC (osobní čísla zaměstnanců)
def read_empl(bl):
    empl = pd.DataFrame( bl['OSC'].unique() )
    for idx, idname in empl.iterrows():
        e1 = pd.DataFrame( bl[bl['OSC'] == idname[0]] )
        empl.at[idx, 'OSC'] = e1.OSC.iloc[0]
        empl.at[idx, 'PRIJMENI'] = e1.PRIJMENI.iloc[0]
        empl.at[idx, 'JMENO'] = e1.JMENO.iloc[0]
        empl.at[idx, 'DRUHPOM'] = e1.DRUHPOM.iloc[0]
    empl = empl.sort_values(by='PRIJMENI')
    iter=0
    for idx_em, em in empl.iterrows():
        empl.at[idx_em, 'IDX'] = iter
        iter+=1
    empl['IDX']=empl['IDX'].astype(int)
    empl['DRUHPOM']=empl['DRUHPOM'].astype(int)
    return empl

# najdi seznam unikátních zakázek podle IDZAKAZKY
def read_zakz_from_bl(bl):
    zakz = pd.DataFrame( bl['IDZAKAZKY'].unique() )
    for idx, idname in zakz.iterrows():
        e1 = pd.DataFrame( bl[bl['IDZAKAZKY'] == idname[0]] )
        zakz.at[idx, 'TA'] = str(e1.TA.iloc[0])
        zakz.at[idx, 'AKCE'] = str(e1.AKCE.iloc[0])
    zakz = zakz.sort_values(by='TA')
    iter=0
    for idx_zak, zak in zakz.iterrows():
        zakz.at[idx_zak, 'IDX'] = iter
        iter+=1
    zakz['IDX'] = zakz['IDX'].astype(int)
    return zakz

list_incorrect_files = []
for filename in glob.glob('*.xls'):	
	if not (filename_prefix in filename and len(filename)==len(filename_prefix)+len(str(year_start))+len('.xls')):
		list_incorrect_files.append(filename)

if len(list_incorrect_files)==0:

    bl = read_data_from_xls()

    empl = read_empl(bl)
    zakz = read_zakz_from_bl(bl)

    # optimize for speed (not memory)
    bl_empl = []
    for idx_em, em in empl.iterrows():
        bl_empl.append(pd.DataFrame(bl[ (bl['OSC']==em['OSC']) ]))

    bl_arr = [[0 for x in range(len(zakz))] for y in range(len(empl))]
    for idx_em, em in empl.iterrows():
        for idx_zak, zak in zakz.iterrows():
            bl_arr[em['IDX']][zak['IDX']]  = bl[ (bl['OSC']==em['OSC']) & (bl['IDZAKAZKY']==zak[0]) ].drop(["PRIJMENI","JMENO","TA","AKCE","IDZAKAZKY","DRUHPOM","OSC"], axis=1, inplace=False)




    # veškerá těžká práce se rovnou  exportuje do xlsx
    with pd.ExcelWriter(excel_file_path, engine='xlsxwriter') as writer:
        # 1 sheet: total
        print("creating xls sheet SUMY")
        out = pd.DataFrame()
        iter=0
        for idx_zak, zak in zakz.iterrows():
            out.at[iter, 'TA'] = zak['TA']
            out.at[iter, 'AKCE'] = zak['AKCE']
            # filter conditions
            # yearly
            for year in range(year_start, year_end+1):
                if years.at['Y', year]==True:
                    out.at[iter, str(year)] = bl[ (bl['IDZAKAZKY']==zak[0]) & (bl['ROK']==year) ].CASTKA.sum()
            out.at[iter,'EMPTY1'] = ""
            # monthly
            for year in range(year_start, year_end+1):
                if years.at['Y', year]==True:
                    for month in range(1,13):
                        out.at[iter, str(year)+"_"+str(month)] = bl[ (bl['IDZAKAZKY']==zak[0]) & (bl['ROK']==year) & (bl['MESIC']==month)].CASTKA.sum()
            iter+=1
        out.at[iter, 'TA'] = ""
        out.at[iter, 'AKCE'] = ""
        iter+=1 # add empty space
        # sum_department
        out.at[iter, 'TA'] = "suma za katedru"
        out.at[iter, 'AKCE'] = ""
        # yearly
        for year in range(year_start, year_end+1):
            if years.at['Y', year]==True:
                out.at[iter, str(year)] = bl[(bl['TA'].isin(group_department)) & (bl['ROK']==year)].CASTKA.sum()
        out.at[iter,'EMPTY1'] = ""
        # monthly
        for year in range(year_start, year_end+1):
            if years.at['Y', year]==True:
                for month in range(1,13):
                    out.at[iter, str(year)+"_"+str(month)] = bl[(bl['TA'].isin(group_department)) & (bl['ROK']==year) & (bl['MESIC']==month)].CASTKA.sum()
                out.at[iter,'EMPTY_'+str(year-year_start)] = ""
        # sum_non_department
        iter+=1
        out.at[iter, 'TA'] = "z projektů a ostatních zdrojů"
        out.at[iter, 'AKCE'] = ""
        # yearly
        for year in range(year_start, year_end+1):
            if years.at['Y', year]==True:
                out.at[iter, str(year)] = bl[(bl['TA'].isin(group_department)==False) & (bl['ROK']==year)].CASTKA.sum()
        out.at[iter,'EMPTY1'] = ""
        # monthly
        for year in range(year_start, year_end+1):
            if years.at['Y', year]==True:
                for month in range(1,13):
                    out.at[iter, str(year)+"_"+str(month)] = bl[(bl['TA'].isin(group_department)==False) & (bl['ROK']==year) & (bl['MESIC']==month)].CASTKA.sum()
                out.at[iter,'EMPTY_'+str(year-year_start)] = ""
        iter+=1
        out.at[iter, 'TA'] = ""
        out.at[iter, 'AKCE'] = ""
        iter+=1 # add empty space
        # sum all
        out.at[iter, 'TA'] = "celkem"
        out.at[iter, 'AKCE'] = ""
        # yearly
        for year in range(year_start, year_end+1):
            if years.at['Y', year]==True:
                out.at[iter, str(year)] = bl[(bl['ROK']==year)].CASTKA.sum()
        out.at[iter,'EMPTY1'] = ""
        # monthly
        for year in range(year_start, year_end+1):
            if years.at['Y', year]==True:
                for month in range(1,13):
                    out.at[iter, str(year)+"_"+str(month)] = bl[(bl['ROK']==year) & (bl['MESIC']==month)].CASTKA.sum()
                out.at[iter,'EMPTY_'+str(year-year_start)] = ""

        out.to_excel(writer, index=False, sheet_name="SUMY")





        # výpis čerpání ze zakázek po lidech a rozdělení zakázek na "za katedru" podle group_department a na "zbytek"
        print("creating xls sheet LIDE")
        out = pd.DataFrame()
        iter=0
        out.at[iter,'PRIJMENI']="za katedru"
        out.at[iter,'JMENO']=""
        # yearly
        for year in range(year_start, year_end+1):
            if years.at['Y', year]==True:
                out.at[iter, str(year)] = bl[ (bl['TA'].isin(group_department)) & (bl['ROK']==year) ].CASTKA.sum()
        out.at[iter,'EMPTY1'] = ""
        # monthly
        for year in range(year_start, year_end+1):
            if years.at['Y', year]==True:
                for month in range(1,13):
                    out.at[iter, str(year)+"_"+str(month)] = bl[ (bl['TA'].isin(group_department)) & (bl['ROK']==year) & (bl['MESIC']==month) ].CASTKA.sum()
                out.at[iter,'EMPTY_'+str(year-year_start)] = ""
        iter+=1
        out.at[iter,'PRIJMENI']=""
        out.at[iter,'JMENO']=""
        iter+=1
        for idx_em, em in empl.iterrows():
            print("    processing "+em['PRIJMENI'])
            out.at[iter, 'PRIJMENI']=em['PRIJMENI']
            out.at[iter, 'JMENO'] = em['JMENO']
            exbl=bl_empl[em['IDX']]
            # yearly
            for year in range(year_start, year_end+1):
                if years.at['Y', year]==True:
                    suma=exbl[(exbl['TA'].isin(group_department)) & (exbl['ROK']==year)].CASTKA.sum()
                    out.at[iter, str(year)] = suma
            out.at[iter,'EMPTY1'] = ""
            # monthly
            for year in range(year_start, year_end+1):
                if years.at['Y', year]==True:
                    for month in range(1,13):
                        suma = bl[(bl['TA'].isin(group_department)) & (bl['OSC']==em['OSC']) & (bl['ROK']==year) & (bl['MESIC']==month)].CASTKA.sum()
                        out.at[iter, str(year)+"_"+str(month)] = suma
                    out.at[iter,'EMPTY_'+str(year-year_start)] = ""
            iter+=1
        out.at[iter,'PRIJMENI']=""
        out.at[iter,'JMENO']=""
        iter+=1
        out.at[iter,'PRIJMENI']="z projektů a ostatních zdrojů"
        out.at[iter,'JMENO']=""
        # yearly
        for year in range(year_start, year_end+1):
            if years.at['Y', year]==True:
                out.at[iter, str(year)] = bl[(bl['TA'].isin(group_department)==False) & (bl['ROK']==year)].CASTKA.sum()
        out.at[iter,'EMPTY1'] = ""
        # monthly
        for year in range(year_start, year_end+1):
            if years.at['Y', year]==True:
                for month in range(1,13):
                    out.at[iter, str(year)+"_"+str(month)] = bl[(bl['TA'].isin(group_department)==False) & (bl['ROK']==year) & (bl['MESIC']==month)].CASTKA.sum()
                out.at[iter,'EMPTY_'+str(year-year_start)] = ""
        iter+=1
        out.at[iter,'PRIJMENI']=""
        out.at[iter,'JMENO']=""
        iter+=1
        for idx_em, em in empl.iterrows():
            out.at[iter, 'PRIJMENI'] = em['PRIJMENI']
            out.at[iter, 'JMENO'] = em['JMENO']
            exbl=bl_empl[em['IDX']]
            # yearly
            for year in range(year_start, year_end+1):
                if years.at['Y', year]==True:
    #                out.at[iter, str(year)] = bl[(bl['TA'].isin(group_department)==False) & (bl['OSC']==em['OSC']) & (bl['ROK']==year)].CASTKA.sum()
                    out.at[iter, str(year)] = exbl[(exbl['TA'].isin(group_department)==False) & (exbl['ROK']==year)].CASTKA.sum()
            out.at[iter,'EMPTY1'] = ""
            # monthly
            for year in range(year_start, year_end+1):
                if years.at['Y', year]==True:
                    for month in range(1,13):
                        out.at[iter, str(year)+"_"+str(month)] = bl[(bl['TA'].isin(group_department)==False) & (bl['OSC']==em['OSC']) & (bl['ROK']==year) & (bl['MESIC']==month)].CASTKA.sum()
                    out.at[iter,'EMPTY_'+str(year-year_start)] = ""
            iter+=1
        out.to_excel(writer, index=False, sheet_name="LIDE")


        """
        # rozdělení čerpání zdrojů po lidech na zdroje z katedry a mimo katedru
        print("creating xls sheet LIDE_1")
        out = pd.DataFrame()
        iter=0
        out.at[iter,'PRIJMENI']="za katedru"
        out.at[iter,'JMENO']=""
        # yearly
        for year in range(year_start, year_end+1):
            if years.at['Y', year]==True:
                out.at[iter, str(year)] = bl[(bl['DRUHPOM']==1) & (bl['TA'].isin(group_department)) & (bl['ROK']==year)].CASTKA.sum()
        out.at[iter,'EMPTY1'] = ""
        # monthly
        for year in range(year_start, year_end+1):
            if years.at['Y', year]==True:
                for month in range(1,13):
                    out.at[iter, str(year)+"_"+str(month)] = bl[(bl['DRUHPOM']==1) & (bl['TA'].isin(group_department)) & (bl['ROK']==year) & (bl['MESIC']==month)].CASTKA.sum()
                out.at[iter,'EMPTY_'+str(year-year_start)] = ""
        iter+=1
        out.at[iter,'PRIJMENI']=""
        out.at[iter,'JMENO']=""
        iter+=1
        for idx_em, em in empl.iterrows():
            if em['DRUHPOM']==1:
                out.at[iter, 'PRIJMENI'] = em['PRIJMENI']
                out.at[iter, 'JMENO'] = em['JMENO']
                # yearly
                for year in range(year_start, year_end+1):
                    if years.at['Y', year]==True:
                        out.at[iter, str(year)] = bl[(bl['DRUHPOM']==1) & (bl['TA'].isin(group_department)) & (bl['OSC']==em['OSC']) & (bl['ROK']==year)].CASTKA.sum()
                out.at[iter,'EMPTY1'] = ""
                # monthly
                for year in range(year_start, year_end+1):
                    if years.at['Y', year]==True:
                        for month in range(1,13):
                            out.at[iter, str(year)+"_"+str(month)] = bl[(bl['DRUHPOM']==1) & (bl['TA'].isin(group_department)) & (bl['OSC']==em['OSC']) & (bl['ROK']==year) & (bl['MESIC']==month)].CASTKA.sum()
                        out.at[iter,'EMPTY_'+str(year-year_start)] = ""
                iter+=1
        out.at[iter,'PRIJMENI']=""
        out.at[iter,'JMENO']=""
        iter+=1
        out.at[iter,'PRIJMENI']="z projektů a ostatních zdrojů"
        out.at[iter,'JMENO']=""
        # yearly
        for year in range(year_start, year_end+1):
            if years.at['Y', year]==True:
                out.at[iter, str(year)] = bl[(bl['DRUHPOM']==1) & (bl['TA'].isin(group_department)==False) & (bl['ROK']==year)].CASTKA.sum()
        out.at[iter,'EMPTY1'] = ""
        # monthly
        for year in range(year_start, year_end+1):
            if years.at['Y', year]==True:
                for month in range(1,13):
                    out.at[iter, str(year)+"_"+str(month)] = bl[(bl['DRUHPOM']==1) & (bl['TA'].isin(group_department)==False) & (bl['ROK']==year) & (bl['MESIC']==month)].CASTKA.sum()
                out.at[iter,'EMPTY_'+str(year-year_start)] = ""
        iter+=1
        out.at[iter,'PRIJMENI']=""
        out.at[iter,'JMENO']=""
        iter+=1
        for idx_em, em in empl.iterrows():
            if em['DRUHPOM']==1:
                out.at[iter, 'PRIJMENI'] = em['PRIJMENI']
                out.at[iter, 'JMENO'] = em['JMENO']
                # yearly
                for year in range(year_start, year_end+1):
                    if years.at['Y', year]==True:
                        out.at[iter, str(year)] = bl[(bl['TA'].isin(group_department)==False) & (bl['PRIJMENI']==em['PRIJMENI']) & (bl['JMENO']==em['JMENO']) & (bl['ROK']==year)].CASTKA.sum()
                out.at[iter,'EMPTY1'] = ""
                # monthly
                for year in range(year_start, year_end+1):
                    if years.at['Y', year]==True:
                        for month in range(1,13):
                            out.at[iter, str(year)+"_"+str(month)] = bl[(bl['TA'].isin(group_department)==False) & (bl['PRIJMENI']==em['PRIJMENI']) & (bl['JMENO']==em['JMENO']) & (bl['ROK']==year) & (bl['MESIC']==month)].CASTKA.sum()
                        out.at[iter,'EMPTY_'+str(year-year_start)] = ""
                iter+=1
        out.to_excel(writer, index=False, sheet_name="LIDE_1")
        """

        
        # nyní se pro každého zaměstnance vytvoří samostatný list v xlsx a do něj nasypou všechny sumy za každou zakázku
        for idx_em, em in empl.iterrows():
            print("creating xls sheet " + str(em['PRIJMENI']) + "_" + str(em['JMENO']))
    #        exbl=bl_empl[em['IDX']]
            out = pd.DataFrame()
            iter=0
            for idx_zak, zak in zakz.iterrows():
                out.at[iter, 'TA'] = zak['TA']
                out.at[iter, 'AKCE'] = zak['AKCE']
                exexbl = bl_arr[em['IDX']][zak['IDX']]
                # yearly
                for year in range(year_start, year_end+1):
                    if years.at['Y', year]==True:
    #                    out.at[iter, str(year)] = bl[ (bl['IDZAKAZKY']==zak[0]) & (bl['PRIJMENI']==em['PRIJMENI']) & (bl['JMENO']==em['JMENO']) & (bl['ROK']==year)].CASTKA.sum()
    #                    out.at[iter, str(year)] = exbl[ (exbl['IDZAKAZKY']==zak[0]) & (exbl['ROK']==year)].CASTKA.sum()
                        # this is actually too slow:
                        #suma = exexbl[ (exexbl['ROK']==year)].CASTKA.sum()
                        suma = 0
                        for idx_ex, ex in exexbl.iterrows():
                            if (ex['ROK']==year):
                                suma+=ex['CASTKA']
                        out.at[iter, str(year)] = suma
                out.at[iter,'EMPTY1'] = ""
                # monthly

                for year in range(year_start, year_end+1):
                    if years.at['Y', year]==True:
                        for month in range(1,13):
    #                        out.at[iter, str(year)+"_"+str(month)] = bl[ (bl['IDZAKAZKY']==zak[0]) & (bl['PRIJMENI']==em['PRIJMENI']) & (bl['JMENO']==em['JMENO']) & (bl['ROK']==year) & (bl['MESIC']==month)].CASTKA.sum()
    #                        out.at[iter, str(year)+"_"+str(month)] = exbl[ (exbl['IDZAKAZKY']==zak[0])  & (exbl['ROK']==year) & (exbl['MESIC']==month)].CASTKA.sum()
                            # this is actually too slow:
                            #suma=exexbl[ (exexbl['ROK']==year) & (exexbl['MESIC']==month)].CASTKA.sum()
                            suma=0
                            for idx_ex, ex in exexbl.iterrows():
                                if (ex['ROK']==year and ex['MESIC']==month):
                                    suma+=ex['CASTKA']
                            out.at[iter, str(year)+"_"+str(month)] = suma
                        out.at[iter,'EMPTY_'+str(year-year_start)] = ""
                iter+=1
            out.to_excel(writer, index=False, sheet_name=str(em['PRIJMENI']) + "_" + str(em['JMENO']))


    # Record the end time
    end_time = time.time()


    # Calculate and print the elapsed time
    elapsed_time = end_time - start_time
    print(f"Elapsed time: {elapsed_time} seconds")
else:
    print('Folder contains excel files with incorrect names:')
    for filename in list_incorrect_files:
        print(filename)
    print('please rename it or remove. Program stops.')