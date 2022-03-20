import pandas as pd


file_path = f"C:/Users/user/Desktop"

def read_csv(file_path, file_name):
    
    try:
        try:
            globals()[file_name] = pd.read_csv(f"{file_path}/{file_name}.csv", encoding='utf-8')

        except:
            sep = "|"
            globals()[file_name] = pd.read_csv(f"{file_path}/{file_name}.csv", sep=sep, encoding='utf-8')
        
        if 'Unnamed: 0' in globals()[file_name].columns or 'level_0' in globals()[file_name].columns:
            try:
                del globals()[file_name]['Unnamed: 0']
            except:
                del globals()[file_name]['level_0']
        
        print("=" * 60)
        print(f'{file_name} / reading_encoding : utf-8')
        print("=" * 60)
        print("\n")

    except:
        globals()[file_name] = pd.read_csv(f"{file_path}/{file_name}.csv", encoding='cp949')
        
        if 'Unnamed: 0' in globals()[file_name].columns or 'level_0' in globals()[file_name].columns:
            try:
                del globals()[file_name]['Unnamed: 0']
            except:
                del globals()[file_name]['level_0']
        
        print("=" * 60)
        print(f'{file_name} / CSV / reading_encoding : cp949')
        print("=" * 60)
        print("\n")
    
    # return globals()[file_name]

def read_excel(file_path, file_name):

    globals()[file_name] = pd.read_excel(f"{file_path}/{file_name}.xlsx")
    
    try:
        globals()[file_name] = globals()[file_name].drop(["Unnamed: 0"], axis=1)
    except:
        pass
    print("=" * 60)
    print(f'{file_name} / EXCEL')
    print("=" * 60)
    print("\n")


def save_csv(df, file_path, file_name, encoding_type):
    
    try:
        if encoding_type == "cp949":
            
            df.to_csv(f"{file_path}/{file_name}_cp949.csv", index=False, encoding="cp949")
            print("=" * 50)
            print("save_encoding : cp949")
            print("=" * 50)
            
            # try:
            #     df.to_csv(f"{file_path}/{file_name}_cp949.csv", index=False, encoding=encoding_type)
            #     print("=" * 50)
            #     print("save_encoding : cp949")
            #     print("=" * 50)
            # except:
            #     df.to_csv(f"{file_path}/{file_name}_utf_sig.csv", index=False, encoding="utf-8-sig")
            #     print("=" * 50)
            #     print("save_encoding : utf-8-sig")
            #     print("=" * 50)

        
        elif encoding_type == "utf-8":
            df.to_csv(f"{file_path}/{file_name}_utf.csv", index=False, sep="|", encoding=encoding_type)
            print("=" * 50)
            print("save_encoding : utf-8")
            print("=" * 50)

    except:
        print("error by encoding")

def save_excel(df, file_path, file_name):
    
    df_1 = df.reset_index().drop("index", axis=1)   
    df_1.to_excel(f"{file_path}/{file_name}.xlsx")
    


def run(file_path):
    
    while True:
        
        read_or_save = input("read or save? : ")
    
        if read_or_save == "read":
            print("\n파일기본경로는 바탕화면 입니다.\n")
            file_lst = input("read할 파일명을 입력하세요 : ").split(",")
            for file_name in file_lst:

                try:
                    read_csv(file_path, file_name)

                except:
                    read_excel(file_path, file_name)
                

                return globals()[file_name]
            break
    
        elif read_or_save == "save":
            print("파일기본경로는 바탕화면 입니다.\n")
            df_lst = input("save할 DataFrame명을 입력하세요 : ").split(",")
            
            file_type = input("csv or excel? ( csv / excel ) : ")
            
            if file_type == "csv":
                encoding_type = input("encoding_type ( cp949 / utf-8 ) : ")
                
                for df_name in df_lst:
                    save_csv(globals()[df_name], file_path, df_name, encoding_type)
            
            elif file_type == "excel":
            
                for df_name in df_lst:
                    save_excel(globals()[df_name], file_path, df_name)

            break
        
        else:
            print("잘못입력하였습니다.")
            continue

if __name__ == "__main__":
    run(file_path)
    



