import requests
from rest_framework import status
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
import json

import os
import openai

class OpenAI_API:
    def __init__(self, api_key):
        self.openai_api_key = api_key
        
    def single_request(self, address_text):
        prompt = f"""Tabular Data Extraction You are a highly intelligent and accurate tabular data extractor from 
            plain text input and especially from emergency text that carries address information, your inputs can be text 
            of arbitrary size, but the output should be in [{{'tabular': {{'entity_type': 'entity'}} }}] JSON format Force it 
            to only extract keys that are shared as an example in the examples section, if a key value is not found in the 
            text input, then it should be ignored. Have only city, distinct, neighbourhood, 
            street, no, tel, name_surname, address Examples:
            
            Input: Deprem sırasında evimizde yer alan adresimiz: İstanbul, Beşiktaş, Yıldız Mahallesi, Cumhuriyet Caddesi No: 35, cep telefonu numaram 5551231256, adim Ahmet Yilmaz 
            Output: {{'city': 'İstanbul', 'distinct': 'Beşiktaş', 'neighbourhood': 'Yıldız Mahallesi', 'street': 'Cumhuriyet Caddesi', 'no': '35', 'tel': '5551231256', 'name_surname': 'Ahmet Yılmaz', 'address': 'İstanbul, Beşiktaş, Yıldız Mahallesi, Cumhuriyet Caddesi No: 35'}}
            
            Input: 5.29 PMO $ 0 87 DEVREMİZ ÖZGÜR ORÇAN ARKADAŞIMIZA ULAŞAMIYORUZ BEYOĞLU MAH FEVZİ ÇAKMAK CAD. NO.58-TÜRKOĞLUI KAHRAMANMARAŞ 5524357578 AdReSe YaKIN OLANLAR VEYA ULASANLAR LÜTFEN BiLGILENDIRSIN .
            Output: {{'city': 'Kahramanmaraş', 'distinct': 'Türkoğlu', 'neighbourhood': 'Beyoğlu Mahallesi', 'street': 'Çakmak Caddesi', 'no': '58', 'tel': '5524357578', 'name_surname': 'Özgür Orçan', 'address': 'Beyoğlu Mahallesi, Çakmak Caddesi, No:58 Türkoğlu/Kahramanmaraş'}}
            
            Input: Ahmet @ozknhmt Ekim 2021 tarihinde katıldı - 2 Takipçi Takip ettiğin kimse takip etmiyor AKEVLER MAH. 432SK RÜYA APT ANT(BEDİİ SABUNCU KARŞISI) ANTAKYA HATAY MERVE BELANLI ses veriyor ancak hiçbiryardım ekibi olmadığı için kurtaramryoruz içeri girip, lütfen acil yardım_ İsim: Merve Belanlı tel 542 757 5484 Ö0 12.07
            Output: {{'city': 'Hatay', 'distinct': 'Antakya', 'neighbourhood': 'Akevler Mahallesi', 'street': '432 Sokak', 'no': '', 'tel': '5427575484', 'name_surname': 'Merve Belanlı', 'address': 'Akevler Mahallesi, 432 Sokak, Rüya Apt. Antakya/Hatay'}}
            
            Input: 14:04 Sümerler Cemil Şükrü Çolokoğlu ilköğretim okulu karşısı 3 9öçük altında yardım bekyouk Lütfen herkes paylogsın
            Output: {{'city': '', 'distinct': '', 'neighbourhood': 'Sümerler Mahallesi', 'street': 'Cemil Şükrü Çolokoğlu İlköğretim Okulu Karşısı', 'no': '', 'tel': '', 'name_surname': '', 'address': 'Sümerler Mahallesi, Cemil Şükrü Çolokoğlu İlköğretim Okulu Karşısı'}}
                        
            Input: {address_text}
            Output:
        """
        
        openai.api_type = "azure"
        openai.api_base = "https://openai-eu.openai.azure.com/"
        openai.api_version = "2022-12-01"
        openai.api_key = self.openai_api_key
        
        response = openai.Completion.create(
          engine="depremyardim",
          prompt=prompt,
          temperature=0.,
          max_tokens=500,
          top_p=1,
          n=1,
          logprobs=0,
          echo=False,
          stop=None,
          frequency_penalty=0,
          presence_penalty=0,
          best_of=1)

        if choices := response.get("choices"):
            processed_address = eval(choices[0]["text"].strip())
            full_address = processed_address.get("address")
            # do we want to save this address when it doesn't even have address?
            if not full_address:
                return
            
            return processed_address
        else:
            {'city': None,
             'distinct': None,
             'neighbourhood': None,
             'street': None,
             'no': None,
             'tel': None,
             'name_surname': None,
             'address': None,
            }
    
    def bulk_request(self, tweet_data):
        thread_list: List[Future] = []
        thread_pool = ThreadPoolExecutor(max_workers=5)
        for tweet in tweet_data:
            thread = thread_pool.submit(self.single_request_zekai, headers=headers, tweet=tweet)
            thread_list.append(thread)

        for _ in as_completed(thread_list, timeout=4):
            pass