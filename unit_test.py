import unittest
from func.xml_csv import *
import os


class Test_func(unittest.TestCase):
    def setUp(self) -> None:
        self.header= ["FinInstrmGnlAttrbts.Id","FinInstrmGnlAttrbts.FullNm","FinInstrmGnlAttrbts.ClssfctnTp",
        "FinInstrmGnlAttrbts.CmmdtyDerivInd","FinInstrmGnlAttrbts.NtnlCcy","Issr"] #tested
        self.zip_name=download_extract_zip("esma.xml","DLTINS")
        storage=xml_to_csv("DLTINS_20210117_01of01.xml",self.header)
        self.final=create_dataset(storage,self.header)

    def test_download(self):
         self.assertEqual(self.zip_name,"DLTINS.zip")
    
    def test_convert(self):
        self.assertEqual(self.final, "DLTINS.csv")




if __name__ == "__main__":
    unittest.main()
