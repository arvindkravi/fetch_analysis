{
 "cells": [
  {
   "cell_type": "markdown",
   "id": "17595710-4b22-41fb-a66b-0cf699926007",
   "metadata": {},
   "source": [
    "# Data Quality Checks"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 2,
   "id": "85eead3a-03c9-4bd2-84db-466c0a8d85bf",
   "metadata": {},
   "outputs": [],
   "source": [
    "# Importing required libraries\n",
    "import json\n",
    "import pandas as pd\n",
    "from datetime import datetime"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 3,
   "id": "cf00be72-a931-4b6b-8a17-af479ce62c20",
   "metadata": {},
   "outputs": [],
   "source": [
    "# Defining a function to read JSON files and convert them to a list of dictionaries\n",
    "# Normalizing JSON data to create DataFrames for receipts, brands, and users\n",
    "def read_json(file_name):\n",
    "    data = []\n",
    "    with open(file_name, 'r') as file:\n",
    "        for line in file:\n",
    "            try:\n",
    "                obj = json.loads(line)\n",
    "                data.append(obj)\n",
    "            except json.JSONDecodeError as e:\n",
    "                print(f\"Error decoding JSON: {e}\")\n",
    "    return data\n",
    "\n",
    "receipts = read_json('Downloads/receipts.json')\n",
    "brands = read_json('Downloads/brands.json')\n",
    "users = read_json('Downloads/users.json')\n",
    "\n",
    "\n",
    "receipts_df = pd.json_normalize(receipts)\n",
    "brands_df = pd.json_normalize(brands)\n",
    "users_df = pd.json_normalize(users)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 4,
   "id": "c2204b76-cf7a-4dcf-8c1f-3da1df595814",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "      bonusPointsEarned                            bonusPointsEarnedReason  \\\n",
      "0                 500.0  Receipt number 2 completed, bonus point schedu...   \n",
      "1                 150.0  Receipt number 5 completed, bonus point schedu...   \n",
      "2                   5.0                         All-receipts receipt bonus   \n",
      "3                   5.0                         All-receipts receipt bonus   \n",
      "4                   5.0                         All-receipts receipt bonus   \n",
      "...                 ...                                                ...   \n",
      "1114               25.0                        COMPLETE_NONPARTNER_RECEIPT   \n",
      "1115                NaN                                                NaN   \n",
      "1116                NaN                                                NaN   \n",
      "1117               25.0                        COMPLETE_NONPARTNER_RECEIPT   \n",
      "1118                NaN                                                NaN   \n",
      "\n",
      "     pointsEarned  purchasedItemCount  \\\n",
      "0           500.0                 5.0   \n",
      "1           150.0                 2.0   \n",
      "2               5                 1.0   \n",
      "3             5.0                 4.0   \n",
      "4             5.0                 2.0   \n",
      "...           ...                 ...   \n",
      "1114         25.0                 2.0   \n",
      "1115          NaN                 NaN   \n",
      "1116          NaN                 NaN   \n",
      "1117         25.0                 2.0   \n",
      "1118          NaN                 NaN   \n",
      "\n",
      "                                 rewardsReceiptItemList rewardsReceiptStatus  \\\n",
      "0     [{'barcode': '4011', 'description': 'ITEM NOT ...             FINISHED   \n",
      "1     [{'barcode': '4011', 'description': 'ITEM NOT ...             FINISHED   \n",
      "2     [{'needsFetchReview': False, 'partnerItemId': ...             REJECTED   \n",
      "3     [{'barcode': '4011', 'description': 'ITEM NOT ...             FINISHED   \n",
      "4     [{'barcode': '4011', 'description': 'ITEM NOT ...             FINISHED   \n",
      "...                                                 ...                  ...   \n",
      "1114  [{'barcode': 'B076FJ92M4', 'description': 'mue...             REJECTED   \n",
      "1115                                                NaN            SUBMITTED   \n",
      "1116                                                NaN            SUBMITTED   \n",
      "1117  [{'barcode': 'B076FJ92M4', 'description': 'mue...             REJECTED   \n",
      "1118                                                NaN            SUBMITTED   \n",
      "\n",
      "     totalSpent                    userId                  _id.$oid  \\\n",
      "0         26.00  5ff1e1eacfcf6c399c274ae6  5ff1e1eb0a720f0523000575   \n",
      "1         11.00  5ff1e194b6a9d73a3a9f1052  5ff1e1bb0a720f052300056b   \n",
      "2         10.00  5ff1e1f1cfcf6c399c274b0b  5ff1e1f10a720f052300057a   \n",
      "3         28.00  5ff1e1eacfcf6c399c274ae6  5ff1e1ee0a7214ada100056f   \n",
      "4          1.00  5ff1e194b6a9d73a3a9f1052  5ff1e1d20a7214ada1000561   \n",
      "...         ...                       ...                       ...   \n",
      "1114      34.96  5fc961c3b8cfca11a077dd33  603cc0630a720fde100003e6   \n",
      "1115        NaN  5fc961c3b8cfca11a077dd33  603d0b710a720fde1000042a   \n",
      "1116        NaN  5fc961c3b8cfca11a077dd33  603cf5290a720fde10000413   \n",
      "1117      34.96  5fc961c3b8cfca11a077dd33  603ce7100a7217c72c000405   \n",
      "1118        NaN  5fc961c3b8cfca11a077dd33  603c4fea0a7217c72c000389   \n",
      "\n",
      "      createDate.$date  dateScanned.$date  finishedDate.$date  \\\n",
      "0        1609687531000      1609687531000        1.609688e+12   \n",
      "1        1609687483000      1609687483000        1.609687e+12   \n",
      "2        1609687537000      1609687537000                 NaN   \n",
      "3        1609687534000      1609687534000        1.609688e+12   \n",
      "4        1609687506000      1609687506000        1.609688e+12   \n",
      "...                ...                ...                 ...   \n",
      "1114     1614594147000      1614594147000                 NaN   \n",
      "1115     1614613361873      1614613361873                 NaN   \n",
      "1116     1614607657664      1614607657664                 NaN   \n",
      "1117     1614604048000      1614604048000                 NaN   \n",
      "1118     1614565354962      1614565354962                 NaN   \n",
      "\n",
      "      modifyDate.$date  pointsAwardedDate.$date  purchaseDate.$date  \n",
      "0        1609687536000             1.609688e+12        1.609632e+12  \n",
      "1        1609687488000             1.609687e+12        1.609601e+12  \n",
      "2        1609687542000                      NaN        1.609632e+12  \n",
      "3        1609687539000             1.609688e+12        1.609632e+12  \n",
      "4        1609687511000             1.609688e+12        1.609601e+12  \n",
      "...                ...                      ...                 ...  \n",
      "1114     1614594148000                      NaN        1.597622e+12  \n",
      "1115     1614613361873                      NaN                 NaN  \n",
      "1116     1614607657664                      NaN                 NaN  \n",
      "1117     1614604049000                      NaN        1.597622e+12  \n",
      "1118     1614565354962                      NaN                 NaN  \n",
      "\n",
      "[1119 rows x 15 columns]\n"
     ]
    }
   ],
   "source": [
    "print(receipts_df)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 5,
   "id": "0049b3d5-8421-4ef1-9624-d3ff91667d64",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "           barcode            category      categoryCode  \\\n",
      "0     511111019862              Baking            BAKING   \n",
      "1     511111519928           Beverages         BEVERAGES   \n",
      "2     511111819905              Baking            BAKING   \n",
      "3     511111519874              Baking            BAKING   \n",
      "4     511111319917      Candy & Sweets  CANDY_AND_SWEETS   \n",
      "...            ...                 ...               ...   \n",
      "1162  511111116752              Baking            BAKING   \n",
      "1163  511111706328  Breakfast & Cereal               NaN   \n",
      "1164  511111416173      Candy & Sweets  CANDY_AND_SWEETS   \n",
      "1165  511111400608             Grocery               NaN   \n",
      "1166  511111019930              Baking            BAKING   \n",
      "\n",
      "                           name topBrand                  _id.$oid  \\\n",
      "0     test brand @1612366101024    False  601ac115be37ce2ead437551   \n",
      "1                     Starbucks    False  601c5460be37ce2ead43755f   \n",
      "2     test brand @1612366146176    False  601ac142be37ce2ead43755d   \n",
      "3     test brand @1612366146051    False  601ac142be37ce2ead43755a   \n",
      "4     test brand @1612366146827    False  601ac142be37ce2ead43755e   \n",
      "...                         ...      ...                       ...   \n",
      "1162  test brand @1601644365844      NaN  5f77274dbe37ce6b592e90c0   \n",
      "1163        Dippin Dots® Cereal      NaN  5dc1fca91dda2c0ad7da64ae   \n",
      "1164  test brand @1598639215217      NaN  5f494c6e04db711dd8fe87e7   \n",
      "1165          LIPTON TEA Leaves    False  5a021611e4b00efe02b02a57   \n",
      "1166  test brand @1613158231643    False  6026d757be37ce6369301468   \n",
      "\n",
      "                  cpg.$id.$oid cpg.$ref                      brandCode  \n",
      "0     601ac114be37ce2ead437550     Cogs                            NaN  \n",
      "1     5332f5fbe4b03c9a25efd0ba     Cogs                      STARBUCKS  \n",
      "2     601ac142be37ce2ead437559     Cogs  TEST BRANDCODE @1612366146176  \n",
      "3     601ac142be37ce2ead437559     Cogs  TEST BRANDCODE @1612366146051  \n",
      "4     5332fa12e4b03c9a25efd1e7     Cogs  TEST BRANDCODE @1612366146827  \n",
      "...                        ...      ...                            ...  \n",
      "1162  5f77274dbe37ce6b592e90bf     Cogs                            NaN  \n",
      "1163  53e10d6368abd3c7065097cc     Cogs             DIPPIN DOTS CEREAL  \n",
      "1164  5332fa12e4b03c9a25efd1e7     Cogs  TEST BRANDCODE @1598639215217  \n",
      "1165  5332f5f6e4b03c9a25efd0b4     Cogs              LIPTON TEA Leaves  \n",
      "1166  6026d757be37ce6369301467     Cogs  TEST BRANDCODE @1613158231644  \n",
      "\n",
      "[1167 rows x 9 columns]\n"
     ]
    }
   ],
   "source": [
    "print(brands_df)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 6,
   "id": "49691aad-f59e-4afb-82de-b7e8328cd956",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "     active         role signUpSource state                  _id.$oid  \\\n",
      "0      True     consumer        Email    WI  5ff1e194b6a9d73a3a9f1052   \n",
      "1      True     consumer        Email    WI  5ff1e194b6a9d73a3a9f1052   \n",
      "2      True     consumer        Email    WI  5ff1e194b6a9d73a3a9f1052   \n",
      "3      True     consumer        Email    WI  5ff1e1eacfcf6c399c274ae6   \n",
      "4      True     consumer        Email    WI  5ff1e194b6a9d73a3a9f1052   \n",
      "..      ...          ...          ...   ...                       ...   \n",
      "490    True  fetch-staff          NaN   NaN  54943462e4b07e684157a532   \n",
      "491    True  fetch-staff          NaN   NaN  54943462e4b07e684157a532   \n",
      "492    True  fetch-staff          NaN   NaN  54943462e4b07e684157a532   \n",
      "493    True  fetch-staff          NaN   NaN  54943462e4b07e684157a532   \n",
      "494    True  fetch-staff          NaN   NaN  54943462e4b07e684157a532   \n",
      "\n",
      "     createdDate.$date  lastLogin.$date  \n",
      "0        1609687444800     1.609688e+12  \n",
      "1        1609687444800     1.609688e+12  \n",
      "2        1609687444800     1.609688e+12  \n",
      "3        1609687530554     1.609688e+12  \n",
      "4        1609687444800     1.609688e+12  \n",
      "..                 ...              ...  \n",
      "490      1418998882381     1.614963e+12  \n",
      "491      1418998882381     1.614963e+12  \n",
      "492      1418998882381     1.614963e+12  \n",
      "493      1418998882381     1.614963e+12  \n",
      "494      1418998882381     1.614963e+12  \n",
      "\n",
      "[495 rows x 7 columns]\n"
     ]
    }
   ],
   "source": [
    "print(users_df)"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "dd5c6e40-efa3-477c-8a3a-a3174929a4e8",
   "metadata": {},
   "source": [
    "# Receipts"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 7,
   "id": "dad278b3-c989-4e83-b8ad-b3efa1c50d87",
   "metadata": {},
   "outputs": [],
   "source": [
    "#Converting columns to numeric format\n",
    "numeric_columns = ['pointsEarned', 'totalSpent']\n",
    "for column in numeric_columns:\n",
    "    receipts_df[column] = pd.to_numeric(receipts_df[column])"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 8,
   "id": "ef977fd4-c803-489f-856a-b004a53a26a7",
   "metadata": {},
   "outputs": [],
   "source": [
    "#Converting 'rewardsReceiptStatus' column to categorical format\n",
    "receipts_df['rewardsReceiptStatus'] = pd.Categorical(receipts_df['rewardsReceiptStatus'])"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 9,
   "id": "f0b1d275-166c-4aca-893a-3f6baa6da202",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "      bonusPointsEarned                            bonusPointsEarnedReason  \\\n",
      "0                 500.0  Receipt number 2 completed, bonus point schedu...   \n",
      "1                 150.0  Receipt number 5 completed, bonus point schedu...   \n",
      "2                   5.0                         All-receipts receipt bonus   \n",
      "3                   5.0                         All-receipts receipt bonus   \n",
      "4                   5.0                         All-receipts receipt bonus   \n",
      "...                 ...                                                ...   \n",
      "1114               25.0                        COMPLETE_NONPARTNER_RECEIPT   \n",
      "1115                NaN                                                NaN   \n",
      "1116                NaN                                                NaN   \n",
      "1117               25.0                        COMPLETE_NONPARTNER_RECEIPT   \n",
      "1118                NaN                                                NaN   \n",
      "\n",
      "      pointsEarned  purchasedItemCount  \\\n",
      "0            500.0                 5.0   \n",
      "1            150.0                 2.0   \n",
      "2              5.0                 1.0   \n",
      "3              5.0                 4.0   \n",
      "4              5.0                 2.0   \n",
      "...            ...                 ...   \n",
      "1114          25.0                 2.0   \n",
      "1115           NaN                 NaN   \n",
      "1116           NaN                 NaN   \n",
      "1117          25.0                 2.0   \n",
      "1118           NaN                 NaN   \n",
      "\n",
      "                                 rewardsReceiptItemList rewardsReceiptStatus  \\\n",
      "0     [{'barcode': '4011', 'description': 'ITEM NOT ...             FINISHED   \n",
      "1     [{'barcode': '4011', 'description': 'ITEM NOT ...             FINISHED   \n",
      "2     [{'needsFetchReview': False, 'partnerItemId': ...             REJECTED   \n",
      "3     [{'barcode': '4011', 'description': 'ITEM NOT ...             FINISHED   \n",
      "4     [{'barcode': '4011', 'description': 'ITEM NOT ...             FINISHED   \n",
      "...                                                 ...                  ...   \n",
      "1114  [{'barcode': 'B076FJ92M4', 'description': 'mue...             REJECTED   \n",
      "1115                                                NaN            SUBMITTED   \n",
      "1116                                                NaN            SUBMITTED   \n",
      "1117  [{'barcode': 'B076FJ92M4', 'description': 'mue...             REJECTED   \n",
      "1118                                                NaN            SUBMITTED   \n",
      "\n",
      "      totalSpent                    userId                  _id.$oid  \\\n",
      "0          26.00  5ff1e1eacfcf6c399c274ae6  5ff1e1eb0a720f0523000575   \n",
      "1          11.00  5ff1e194b6a9d73a3a9f1052  5ff1e1bb0a720f052300056b   \n",
      "2          10.00  5ff1e1f1cfcf6c399c274b0b  5ff1e1f10a720f052300057a   \n",
      "3          28.00  5ff1e1eacfcf6c399c274ae6  5ff1e1ee0a7214ada100056f   \n",
      "4           1.00  5ff1e194b6a9d73a3a9f1052  5ff1e1d20a7214ada1000561   \n",
      "...          ...                       ...                       ...   \n",
      "1114       34.96  5fc961c3b8cfca11a077dd33  603cc0630a720fde100003e6   \n",
      "1115         NaN  5fc961c3b8cfca11a077dd33  603d0b710a720fde1000042a   \n",
      "1116         NaN  5fc961c3b8cfca11a077dd33  603cf5290a720fde10000413   \n",
      "1117       34.96  5fc961c3b8cfca11a077dd33  603ce7100a7217c72c000405   \n",
      "1118         NaN  5fc961c3b8cfca11a077dd33  603c4fea0a7217c72c000389   \n",
      "\n",
      "         createDate.$date    dateScanned.$date   finishedDate.$date  \\\n",
      "0     2021-01-03 10:25:31  2021-01-03 10:25:31  2021-01-03 10:25:31   \n",
      "1     2021-01-03 10:24:43  2021-01-03 10:24:43  2021-01-03 10:24:43   \n",
      "2     2021-01-03 10:25:37  2021-01-03 10:25:37                 None   \n",
      "3     2021-01-03 10:25:34  2021-01-03 10:25:34  2021-01-03 10:25:34   \n",
      "4     2021-01-03 10:25:06  2021-01-03 10:25:06  2021-01-03 10:25:11   \n",
      "...                   ...                  ...                  ...   \n",
      "1114  2021-03-01 05:22:27  2021-03-01 05:22:27                 None   \n",
      "1115  2021-03-01 10:42:41  2021-03-01 10:42:41                 None   \n",
      "1116  2021-03-01 09:07:37  2021-03-01 09:07:37                 None   \n",
      "1117  2021-03-01 08:07:28  2021-03-01 08:07:28                 None   \n",
      "1118  2021-02-28 21:22:34  2021-02-28 21:22:34                 None   \n",
      "\n",
      "         modifyDate.$date pointsAwardedDate.$date   purchaseDate.$date  \n",
      "0     2021-01-03 10:25:36     2021-01-03 10:25:31  2021-01-02 19:00:00  \n",
      "1     2021-01-03 10:24:48     2021-01-03 10:24:43  2021-01-02 10:24:43  \n",
      "2     2021-01-03 10:25:42                    None  2021-01-02 19:00:00  \n",
      "3     2021-01-03 10:25:39     2021-01-03 10:25:34  2021-01-02 19:00:00  \n",
      "4     2021-01-03 10:25:11     2021-01-03 10:25:06  2021-01-02 10:25:06  \n",
      "...                   ...                     ...                  ...  \n",
      "1114  2021-03-01 05:22:28                    None  2020-08-16 20:00:00  \n",
      "1115  2021-03-01 10:42:41                    None                 None  \n",
      "1116  2021-03-01 09:07:37                    None                 None  \n",
      "1117  2021-03-01 08:07:29                    None  2020-08-16 20:00:00  \n",
      "1118  2021-02-28 21:22:34                    None                 None  \n",
      "\n",
      "[1119 rows x 15 columns]\n"
     ]
    }
   ],
   "source": [
    "#Converting timestamp values to datetime format\n",
    "def convert_timestamp(timestamp_ms):\n",
    "    if pd.isna(timestamp_ms):\n",
    "        return None\n",
    "    timestamp_s = timestamp_ms / 1000.0   \n",
    "    date_time = datetime.fromtimestamp(timestamp_s)  \n",
    "    return date_time.strftime('%Y-%m-%d %H:%M:%S')\n",
    "\n",
    "\n",
    "receipts_df['createDate.$date'] = pd.to_numeric(receipts_df['createDate.$date'])\n",
    "receipts_df['createDate.$date']  = receipts_df['createDate.$date'].apply(convert_timestamp)\n",
    "\n",
    "receipts_df['dateScanned.$date'] = pd.to_numeric(receipts_df['dateScanned.$date'])\n",
    "receipts_df['dateScanned.$date']  = receipts_df['dateScanned.$date'].apply(convert_timestamp)\n",
    "\n",
    "receipts_df['finishedDate.$date'] = pd.to_numeric(receipts_df['finishedDate.$date'])\n",
    "receipts_df['finishedDate.$date']  = receipts_df['finishedDate.$date'].apply(convert_timestamp)\n",
    "\n",
    "receipts_df['modifyDate.$date'] = pd.to_numeric(receipts_df['modifyDate.$date'])\n",
    "receipts_df['modifyDate.$date']  = receipts_df['modifyDate.$date'].apply(convert_timestamp)\n",
    "\n",
    "receipts_df['pointsAwardedDate.$date'] = pd.to_numeric(receipts_df['pointsAwardedDate.$date'])\n",
    "receipts_df['pointsAwardedDate.$date']  = receipts_df['pointsAwardedDate.$date'].apply(convert_timestamp)\n",
    "\n",
    "receipts_df['purchaseDate.$date'] = pd.to_numeric(receipts_df['purchaseDate.$date'])\n",
    "receipts_df['purchaseDate.$date']  = receipts_df['purchaseDate.$date'].apply(convert_timestamp)\n",
    "\n",
    "print(receipts_df)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 10,
   "id": "8729a1d5-da23-4d47-a9e4-235b73065ac0",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/html": [
       "<div>\n",
       "<style scoped>\n",
       "    .dataframe tbody tr th:only-of-type {\n",
       "        vertical-align: middle;\n",
       "    }\n",
       "\n",
       "    .dataframe tbody tr th {\n",
       "        vertical-align: top;\n",
       "    }\n",
       "\n",
       "    .dataframe thead th {\n",
       "        text-align: right;\n",
       "    }\n",
       "</style>\n",
       "<table border=\"1\" class=\"dataframe\">\n",
       "  <thead>\n",
       "    <tr style=\"text-align: right;\">\n",
       "      <th></th>\n",
       "      <th>bonusPointsEarned</th>\n",
       "      <th>bonusPointsEarnedReason</th>\n",
       "      <th>pointsEarned</th>\n",
       "      <th>purchasedItemCount</th>\n",
       "      <th>rewardsReceiptItemList</th>\n",
       "      <th>rewardsReceiptStatus</th>\n",
       "      <th>totalSpent</th>\n",
       "      <th>userId</th>\n",
       "      <th>_id.$oid</th>\n",
       "      <th>createDate.$date</th>\n",
       "      <th>dateScanned.$date</th>\n",
       "      <th>finishedDate.$date</th>\n",
       "      <th>modifyDate.$date</th>\n",
       "      <th>pointsAwardedDate.$date</th>\n",
       "      <th>purchaseDate.$date</th>\n",
       "    </tr>\n",
       "  </thead>\n",
       "  <tbody>\n",
       "    <tr>\n",
       "      <th>0</th>\n",
       "      <td>500.0</td>\n",
       "      <td>Receipt number 2 completed, bonus point schedu...</td>\n",
       "      <td>500.0</td>\n",
       "      <td>5.0</td>\n",
       "      <td>[{'barcode': '4011', 'description': 'ITEM NOT ...</td>\n",
       "      <td>FINISHED</td>\n",
       "      <td>26.0</td>\n",
       "      <td>5ff1e1eacfcf6c399c274ae6</td>\n",
       "      <td>5ff1e1eb0a720f0523000575</td>\n",
       "      <td>2021-01-03 10:25:31</td>\n",
       "      <td>2021-01-03 10:25:31</td>\n",
       "      <td>2021-01-03 10:25:31</td>\n",
       "      <td>2021-01-03 10:25:36</td>\n",
       "      <td>2021-01-03 10:25:31</td>\n",
       "      <td>2021-01-02 19:00:00</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>1</th>\n",
       "      <td>150.0</td>\n",
       "      <td>Receipt number 5 completed, bonus point schedu...</td>\n",
       "      <td>150.0</td>\n",
       "      <td>2.0</td>\n",
       "      <td>[{'barcode': '4011', 'description': 'ITEM NOT ...</td>\n",
       "      <td>FINISHED</td>\n",
       "      <td>11.0</td>\n",
       "      <td>5ff1e194b6a9d73a3a9f1052</td>\n",
       "      <td>5ff1e1bb0a720f052300056b</td>\n",
       "      <td>2021-01-03 10:24:43</td>\n",
       "      <td>2021-01-03 10:24:43</td>\n",
       "      <td>2021-01-03 10:24:43</td>\n",
       "      <td>2021-01-03 10:24:48</td>\n",
       "      <td>2021-01-03 10:24:43</td>\n",
       "      <td>2021-01-02 10:24:43</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>2</th>\n",
       "      <td>5.0</td>\n",
       "      <td>All-receipts receipt bonus</td>\n",
       "      <td>5.0</td>\n",
       "      <td>1.0</td>\n",
       "      <td>[{'needsFetchReview': False, 'partnerItemId': ...</td>\n",
       "      <td>REJECTED</td>\n",
       "      <td>10.0</td>\n",
       "      <td>5ff1e1f1cfcf6c399c274b0b</td>\n",
       "      <td>5ff1e1f10a720f052300057a</td>\n",
       "      <td>2021-01-03 10:25:37</td>\n",
       "      <td>2021-01-03 10:25:37</td>\n",
       "      <td>None</td>\n",
       "      <td>2021-01-03 10:25:42</td>\n",
       "      <td>None</td>\n",
       "      <td>2021-01-02 19:00:00</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>3</th>\n",
       "      <td>5.0</td>\n",
       "      <td>All-receipts receipt bonus</td>\n",
       "      <td>5.0</td>\n",
       "      <td>4.0</td>\n",
       "      <td>[{'barcode': '4011', 'description': 'ITEM NOT ...</td>\n",
       "      <td>FINISHED</td>\n",
       "      <td>28.0</td>\n",
       "      <td>5ff1e1eacfcf6c399c274ae6</td>\n",
       "      <td>5ff1e1ee0a7214ada100056f</td>\n",
       "      <td>2021-01-03 10:25:34</td>\n",
       "      <td>2021-01-03 10:25:34</td>\n",
       "      <td>2021-01-03 10:25:34</td>\n",
       "      <td>2021-01-03 10:25:39</td>\n",
       "      <td>2021-01-03 10:25:34</td>\n",
       "      <td>2021-01-02 19:00:00</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>4</th>\n",
       "      <td>5.0</td>\n",
       "      <td>All-receipts receipt bonus</td>\n",
       "      <td>5.0</td>\n",
       "      <td>2.0</td>\n",
       "      <td>[{'barcode': '4011', 'description': 'ITEM NOT ...</td>\n",
       "      <td>FINISHED</td>\n",
       "      <td>1.0</td>\n",
       "      <td>5ff1e194b6a9d73a3a9f1052</td>\n",
       "      <td>5ff1e1d20a7214ada1000561</td>\n",
       "      <td>2021-01-03 10:25:06</td>\n",
       "      <td>2021-01-03 10:25:06</td>\n",
       "      <td>2021-01-03 10:25:11</td>\n",
       "      <td>2021-01-03 10:25:11</td>\n",
       "      <td>2021-01-03 10:25:06</td>\n",
       "      <td>2021-01-02 10:25:06</td>\n",
       "    </tr>\n",
       "  </tbody>\n",
       "</table>\n",
       "</div>"
      ],
      "text/plain": [
       "   bonusPointsEarned                            bonusPointsEarnedReason  \\\n",
       "0              500.0  Receipt number 2 completed, bonus point schedu...   \n",
       "1              150.0  Receipt number 5 completed, bonus point schedu...   \n",
       "2                5.0                         All-receipts receipt bonus   \n",
       "3                5.0                         All-receipts receipt bonus   \n",
       "4                5.0                         All-receipts receipt bonus   \n",
       "\n",
       "   pointsEarned  purchasedItemCount  \\\n",
       "0         500.0                 5.0   \n",
       "1         150.0                 2.0   \n",
       "2           5.0                 1.0   \n",
       "3           5.0                 4.0   \n",
       "4           5.0                 2.0   \n",
       "\n",
       "                              rewardsReceiptItemList rewardsReceiptStatus  \\\n",
       "0  [{'barcode': '4011', 'description': 'ITEM NOT ...             FINISHED   \n",
       "1  [{'barcode': '4011', 'description': 'ITEM NOT ...             FINISHED   \n",
       "2  [{'needsFetchReview': False, 'partnerItemId': ...             REJECTED   \n",
       "3  [{'barcode': '4011', 'description': 'ITEM NOT ...             FINISHED   \n",
       "4  [{'barcode': '4011', 'description': 'ITEM NOT ...             FINISHED   \n",
       "\n",
       "   totalSpent                    userId                  _id.$oid  \\\n",
       "0        26.0  5ff1e1eacfcf6c399c274ae6  5ff1e1eb0a720f0523000575   \n",
       "1        11.0  5ff1e194b6a9d73a3a9f1052  5ff1e1bb0a720f052300056b   \n",
       "2        10.0  5ff1e1f1cfcf6c399c274b0b  5ff1e1f10a720f052300057a   \n",
       "3        28.0  5ff1e1eacfcf6c399c274ae6  5ff1e1ee0a7214ada100056f   \n",
       "4         1.0  5ff1e194b6a9d73a3a9f1052  5ff1e1d20a7214ada1000561   \n",
       "\n",
       "      createDate.$date    dateScanned.$date   finishedDate.$date  \\\n",
       "0  2021-01-03 10:25:31  2021-01-03 10:25:31  2021-01-03 10:25:31   \n",
       "1  2021-01-03 10:24:43  2021-01-03 10:24:43  2021-01-03 10:24:43   \n",
       "2  2021-01-03 10:25:37  2021-01-03 10:25:37                 None   \n",
       "3  2021-01-03 10:25:34  2021-01-03 10:25:34  2021-01-03 10:25:34   \n",
       "4  2021-01-03 10:25:06  2021-01-03 10:25:06  2021-01-03 10:25:11   \n",
       "\n",
       "      modifyDate.$date pointsAwardedDate.$date   purchaseDate.$date  \n",
       "0  2021-01-03 10:25:36     2021-01-03 10:25:31  2021-01-02 19:00:00  \n",
       "1  2021-01-03 10:24:48     2021-01-03 10:24:43  2021-01-02 10:24:43  \n",
       "2  2021-01-03 10:25:42                    None  2021-01-02 19:00:00  \n",
       "3  2021-01-03 10:25:39     2021-01-03 10:25:34  2021-01-02 19:00:00  \n",
       "4  2021-01-03 10:25:11     2021-01-03 10:25:06  2021-01-02 10:25:06  "
      ]
     },
     "execution_count": 10,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "receipts_df.head()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 11,
   "id": "4f5a0233-8b1d-4488-91bb-e45b6f2a956c",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "(1119, 15)"
      ]
     },
     "execution_count": 11,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "receipts_df.shape"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 12,
   "id": "56a9b665-a1ab-4ca8-9a0f-fc7216918095",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/html": [
       "<div>\n",
       "<style scoped>\n",
       "    .dataframe tbody tr th:only-of-type {\n",
       "        vertical-align: middle;\n",
       "    }\n",
       "\n",
       "    .dataframe tbody tr th {\n",
       "        vertical-align: top;\n",
       "    }\n",
       "\n",
       "    .dataframe thead th {\n",
       "        text-align: right;\n",
       "    }\n",
       "</style>\n",
       "<table border=\"1\" class=\"dataframe\">\n",
       "  <thead>\n",
       "    <tr style=\"text-align: right;\">\n",
       "      <th></th>\n",
       "      <th>bonusPointsEarned</th>\n",
       "      <th>pointsEarned</th>\n",
       "      <th>purchasedItemCount</th>\n",
       "      <th>totalSpent</th>\n",
       "    </tr>\n",
       "  </thead>\n",
       "  <tbody>\n",
       "    <tr>\n",
       "      <th>count</th>\n",
       "      <td>544.000000</td>\n",
       "      <td>609.000000</td>\n",
       "      <td>635.00000</td>\n",
       "      <td>684.000000</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>mean</th>\n",
       "      <td>238.893382</td>\n",
       "      <td>585.962890</td>\n",
       "      <td>14.75748</td>\n",
       "      <td>77.796857</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>std</th>\n",
       "      <td>299.091731</td>\n",
       "      <td>1357.166947</td>\n",
       "      <td>61.13424</td>\n",
       "      <td>347.110349</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>min</th>\n",
       "      <td>5.000000</td>\n",
       "      <td>0.000000</td>\n",
       "      <td>0.00000</td>\n",
       "      <td>0.000000</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>25%</th>\n",
       "      <td>5.000000</td>\n",
       "      <td>5.000000</td>\n",
       "      <td>1.00000</td>\n",
       "      <td>1.000000</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>50%</th>\n",
       "      <td>45.000000</td>\n",
       "      <td>150.000000</td>\n",
       "      <td>2.00000</td>\n",
       "      <td>18.200000</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>75%</th>\n",
       "      <td>500.000000</td>\n",
       "      <td>750.000000</td>\n",
       "      <td>5.00000</td>\n",
       "      <td>34.960000</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>max</th>\n",
       "      <td>750.000000</td>\n",
       "      <td>10199.800000</td>\n",
       "      <td>689.00000</td>\n",
       "      <td>4721.950000</td>\n",
       "    </tr>\n",
       "  </tbody>\n",
       "</table>\n",
       "</div>"
      ],
      "text/plain": [
       "       bonusPointsEarned  pointsEarned  purchasedItemCount   totalSpent\n",
       "count         544.000000    609.000000           635.00000   684.000000\n",
       "mean          238.893382    585.962890            14.75748    77.796857\n",
       "std           299.091731   1357.166947            61.13424   347.110349\n",
       "min             5.000000      0.000000             0.00000     0.000000\n",
       "25%             5.000000      5.000000             1.00000     1.000000\n",
       "50%            45.000000    150.000000             2.00000    18.200000\n",
       "75%           500.000000    750.000000             5.00000    34.960000\n",
       "max           750.000000  10199.800000           689.00000  4721.950000"
      ]
     },
     "execution_count": 12,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "# Generating descriptive statistics for the DataFrame\n",
    "receipts_df.describe()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 13,
   "id": "14ba0442-a55f-4f51-bc48-c6c4f0a6e656",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "bonusPointsEarned           float64\n",
       "bonusPointsEarnedReason      object\n",
       "pointsEarned                float64\n",
       "purchasedItemCount          float64\n",
       "rewardsReceiptItemList       object\n",
       "rewardsReceiptStatus       category\n",
       "totalSpent                  float64\n",
       "userId                       object\n",
       "_id.$oid                     object\n",
       "createDate.$date             object\n",
       "dateScanned.$date            object\n",
       "finishedDate.$date           object\n",
       "modifyDate.$date             object\n",
       "pointsAwardedDate.$date      object\n",
       "purchaseDate.$date           object\n",
       "dtype: object"
      ]
     },
     "execution_count": 13,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "# Displaying data types of columns in the DataFrame\n",
    "receipts_df.dtypes"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 14,
   "id": "129a9739-d2a9-4954-9ed2-026ced6b13e7",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "bonusPointsEarned          575\n",
       "bonusPointsEarnedReason    575\n",
       "pointsEarned               510\n",
       "purchasedItemCount         484\n",
       "rewardsReceiptItemList     440\n",
       "rewardsReceiptStatus         0\n",
       "totalSpent                 435\n",
       "userId                       0\n",
       "_id.$oid                     0\n",
       "createDate.$date             0\n",
       "dateScanned.$date            0\n",
       "finishedDate.$date         551\n",
       "modifyDate.$date             0\n",
       "pointsAwardedDate.$date    582\n",
       "purchaseDate.$date         448\n",
       "dtype: int64"
      ]
     },
     "execution_count": 14,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "# Counting missing values in each column of the DataFrame\n",
    "receipts_df.isna().sum()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 15,
   "id": "b6f3f1a3-a709-45ec-b273-a2d2ea7716be",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "461\n"
     ]
    }
   ],
   "source": [
    "# Calculating the count where purchase date is missing or greater than date scanned\n",
    "purchaseDate_dateScanned_count = len(receipts_df[\n",
    "    (receipts_df['purchaseDate.$date'].isnull()) |\n",
    "    (receipts_df['purchaseDate.$date'] > receipts_df['dateScanned.$date'])\n",
    "])\n",
    "\n",
    "print(purchaseDate_dateScanned_count)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 16,
   "id": "6d189f22-2b55-46fa-a42d-c9c2c29221bf",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "551\n"
     ]
    }
   ],
   "source": [
    "# Calculating the count where finished date is missing or date scanned is greater\n",
    "finishedDate_dateScanned_count = len(receipts_df[\n",
    "    (receipts_df['finishedDate.$date'].isnull()) |\n",
    "    (receipts_df['dateScanned.$date'] > receipts_df['finishedDate.$date'])\n",
    "])\n",
    "\n",
    "print(finishedDate_dateScanned_count)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 17,
   "id": "caf39295-9350-4586-82ce-040759823e60",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Minimum purchasedItemCount: 0.0\n",
      "Maximum purchasedItemCount: 689.0\n",
      "Minimum totalSpent: 0.0\n",
      "Maximum totalSpent: 4721.95\n"
     ]
    }
   ],
   "source": [
    "# Filling missing values with -1 for 'purchasedItemCount' and 'totalSpent'\n",
    "# Calculating minimum and maximum values for 'purchasedItemCount' and 'totalSpent'\n",
    "receipts_df['purchasedItemCount'].fillna(-1, inplace=True)\n",
    "receipts_df['totalSpent'].fillna(-1, inplace=True)\n",
    "\n",
    "min_purchasedItemCount = receipts_df[receipts_df['purchasedItemCount'] != -1]['purchasedItemCount'].min()\n",
    "max_purchasedItemCount = receipts_df[receipts_df['purchasedItemCount'] != -1]['purchasedItemCount'].max()\n",
    "\n",
    "min_totalSpent = receipts_df[receipts_df['totalSpent'] != -1]['totalSpent'].min()\n",
    "max_totalSpent = receipts_df[receipts_df['totalSpent'] != -1]['totalSpent'].max()\n",
    "\n",
    "print(\"Minimum purchasedItemCount:\", min_purchasedItemCount)\n",
    "print(\"Maximum purchasedItemCount:\", max_purchasedItemCount)\n",
    "print(\"Minimum totalSpent:\", min_totalSpent)\n",
    "print(\"Maximum totalSpent:\", max_totalSpent)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 18,
   "id": "767da696-f630-4cd9-b877-a4f171a9aa88",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Count of negative values in 'pointsEarned' column: 0\n",
      "Count of negative values in 'bonusPointsEarned' column: 0\n"
     ]
    }
   ],
   "source": [
    "# Counting negative values in 'pointsEarned' and 'bonusPointsEarned' columns\n",
    "negative_pointsEarned_count = (receipts_df['pointsEarned'] < 0).sum()\n",
    "negative_bonusPointsEarned_count = (receipts_df['bonusPointsEarned'] < 0).sum()\n",
    "\n",
    "print(\"Count of negative values in 'pointsEarned' column:\", negative_pointsEarned_count)\n",
    "print(\"Count of negative values in 'bonusPointsEarned' column:\", negative_bonusPointsEarned_count)"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "84ed2dab-025d-4f2d-89d0-0237dc1b45ff",
   "metadata": {},
   "source": [
    "# Brands"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 19,
   "id": "a467df05-e315-4455-892b-af0e9695f12b",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/html": [
       "<div>\n",
       "<style scoped>\n",
       "    .dataframe tbody tr th:only-of-type {\n",
       "        vertical-align: middle;\n",
       "    }\n",
       "\n",
       "    .dataframe tbody tr th {\n",
       "        vertical-align: top;\n",
       "    }\n",
       "\n",
       "    .dataframe thead th {\n",
       "        text-align: right;\n",
       "    }\n",
       "</style>\n",
       "<table border=\"1\" class=\"dataframe\">\n",
       "  <thead>\n",
       "    <tr style=\"text-align: right;\">\n",
       "      <th></th>\n",
       "      <th>barcode</th>\n",
       "      <th>category</th>\n",
       "      <th>categoryCode</th>\n",
       "      <th>name</th>\n",
       "      <th>topBrand</th>\n",
       "      <th>_id.$oid</th>\n",
       "      <th>cpg.$id.$oid</th>\n",
       "      <th>cpg.$ref</th>\n",
       "      <th>brandCode</th>\n",
       "    </tr>\n",
       "  </thead>\n",
       "  <tbody>\n",
       "    <tr>\n",
       "      <th>0</th>\n",
       "      <td>511111019862</td>\n",
       "      <td>Baking</td>\n",
       "      <td>BAKING</td>\n",
       "      <td>test brand @1612366101024</td>\n",
       "      <td>False</td>\n",
       "      <td>601ac115be37ce2ead437551</td>\n",
       "      <td>601ac114be37ce2ead437550</td>\n",
       "      <td>Cogs</td>\n",
       "      <td>NaN</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>1</th>\n",
       "      <td>511111519928</td>\n",
       "      <td>Beverages</td>\n",
       "      <td>BEVERAGES</td>\n",
       "      <td>Starbucks</td>\n",
       "      <td>False</td>\n",
       "      <td>601c5460be37ce2ead43755f</td>\n",
       "      <td>5332f5fbe4b03c9a25efd0ba</td>\n",
       "      <td>Cogs</td>\n",
       "      <td>STARBUCKS</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>2</th>\n",
       "      <td>511111819905</td>\n",
       "      <td>Baking</td>\n",
       "      <td>BAKING</td>\n",
       "      <td>test brand @1612366146176</td>\n",
       "      <td>False</td>\n",
       "      <td>601ac142be37ce2ead43755d</td>\n",
       "      <td>601ac142be37ce2ead437559</td>\n",
       "      <td>Cogs</td>\n",
       "      <td>TEST BRANDCODE @1612366146176</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>3</th>\n",
       "      <td>511111519874</td>\n",
       "      <td>Baking</td>\n",
       "      <td>BAKING</td>\n",
       "      <td>test brand @1612366146051</td>\n",
       "      <td>False</td>\n",
       "      <td>601ac142be37ce2ead43755a</td>\n",
       "      <td>601ac142be37ce2ead437559</td>\n",
       "      <td>Cogs</td>\n",
       "      <td>TEST BRANDCODE @1612366146051</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>4</th>\n",
       "      <td>511111319917</td>\n",
       "      <td>Candy &amp; Sweets</td>\n",
       "      <td>CANDY_AND_SWEETS</td>\n",
       "      <td>test brand @1612366146827</td>\n",
       "      <td>False</td>\n",
       "      <td>601ac142be37ce2ead43755e</td>\n",
       "      <td>5332fa12e4b03c9a25efd1e7</td>\n",
       "      <td>Cogs</td>\n",
       "      <td>TEST BRANDCODE @1612366146827</td>\n",
       "    </tr>\n",
       "  </tbody>\n",
       "</table>\n",
       "</div>"
      ],
      "text/plain": [
       "        barcode        category      categoryCode                       name  \\\n",
       "0  511111019862          Baking            BAKING  test brand @1612366101024   \n",
       "1  511111519928       Beverages         BEVERAGES                  Starbucks   \n",
       "2  511111819905          Baking            BAKING  test brand @1612366146176   \n",
       "3  511111519874          Baking            BAKING  test brand @1612366146051   \n",
       "4  511111319917  Candy & Sweets  CANDY_AND_SWEETS  test brand @1612366146827   \n",
       "\n",
       "  topBrand                  _id.$oid              cpg.$id.$oid cpg.$ref  \\\n",
       "0    False  601ac115be37ce2ead437551  601ac114be37ce2ead437550     Cogs   \n",
       "1    False  601c5460be37ce2ead43755f  5332f5fbe4b03c9a25efd0ba     Cogs   \n",
       "2    False  601ac142be37ce2ead43755d  601ac142be37ce2ead437559     Cogs   \n",
       "3    False  601ac142be37ce2ead43755a  601ac142be37ce2ead437559     Cogs   \n",
       "4    False  601ac142be37ce2ead43755e  5332fa12e4b03c9a25efd1e7     Cogs   \n",
       "\n",
       "                       brandCode  \n",
       "0                            NaN  \n",
       "1                      STARBUCKS  \n",
       "2  TEST BRANDCODE @1612366146176  \n",
       "3  TEST BRANDCODE @1612366146051  \n",
       "4  TEST BRANDCODE @1612366146827  "
      ]
     },
     "execution_count": 19,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "brands_df.head()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 20,
   "id": "d5071751-bfe8-4e92-a442-a2979a703237",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "(1167, 9)"
      ]
     },
     "execution_count": 20,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "# Number of rows and columns of the DataFrame\n",
    "brands_df.shape"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 21,
   "id": "6aea3477-bdca-47b9-a7de-5c3f0e6d74ed",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/html": [
       "<div>\n",
       "<style scoped>\n",
       "    .dataframe tbody tr th:only-of-type {\n",
       "        vertical-align: middle;\n",
       "    }\n",
       "\n",
       "    .dataframe tbody tr th {\n",
       "        vertical-align: top;\n",
       "    }\n",
       "\n",
       "    .dataframe thead th {\n",
       "        text-align: right;\n",
       "    }\n",
       "</style>\n",
       "<table border=\"1\" class=\"dataframe\">\n",
       "  <thead>\n",
       "    <tr style=\"text-align: right;\">\n",
       "      <th></th>\n",
       "      <th>barcode</th>\n",
       "      <th>category</th>\n",
       "      <th>categoryCode</th>\n",
       "      <th>name</th>\n",
       "      <th>topBrand</th>\n",
       "      <th>_id.$oid</th>\n",
       "      <th>cpg.$id.$oid</th>\n",
       "      <th>cpg.$ref</th>\n",
       "      <th>brandCode</th>\n",
       "    </tr>\n",
       "  </thead>\n",
       "  <tbody>\n",
       "    <tr>\n",
       "      <th>count</th>\n",
       "      <td>1167</td>\n",
       "      <td>1012</td>\n",
       "      <td>517</td>\n",
       "      <td>1167</td>\n",
       "      <td>555</td>\n",
       "      <td>1167</td>\n",
       "      <td>1167</td>\n",
       "      <td>1167</td>\n",
       "      <td>933</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>unique</th>\n",
       "      <td>1160</td>\n",
       "      <td>23</td>\n",
       "      <td>14</td>\n",
       "      <td>1156</td>\n",
       "      <td>2</td>\n",
       "      <td>1167</td>\n",
       "      <td>196</td>\n",
       "      <td>2</td>\n",
       "      <td>897</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>top</th>\n",
       "      <td>511111305125</td>\n",
       "      <td>Baking</td>\n",
       "      <td>BAKING</td>\n",
       "      <td>Huggies</td>\n",
       "      <td>False</td>\n",
       "      <td>601ac115be37ce2ead437551</td>\n",
       "      <td>559c2234e4b06aca36af13c6</td>\n",
       "      <td>Cogs</td>\n",
       "      <td></td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>freq</th>\n",
       "      <td>2</td>\n",
       "      <td>369</td>\n",
       "      <td>359</td>\n",
       "      <td>2</td>\n",
       "      <td>524</td>\n",
       "      <td>1</td>\n",
       "      <td>98</td>\n",
       "      <td>1020</td>\n",
       "      <td>35</td>\n",
       "    </tr>\n",
       "  </tbody>\n",
       "</table>\n",
       "</div>"
      ],
      "text/plain": [
       "             barcode category categoryCode     name topBrand  \\\n",
       "count           1167     1012          517     1167      555   \n",
       "unique          1160       23           14     1156        2   \n",
       "top     511111305125   Baking       BAKING  Huggies    False   \n",
       "freq               2      369          359        2      524   \n",
       "\n",
       "                        _id.$oid              cpg.$id.$oid cpg.$ref brandCode  \n",
       "count                       1167                      1167     1167       933  \n",
       "unique                      1167                       196        2       897  \n",
       "top     601ac115be37ce2ead437551  559c2234e4b06aca36af13c6     Cogs            \n",
       "freq                           1                        98     1020        35  "
      ]
     },
     "execution_count": 21,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "# Generating descriptive statistics for the DataFrame\n",
    "brands_df.describe()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 22,
   "id": "9d6d4d5a-25a7-491a-8aa7-cec13fe6c42d",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "barcode         object\n",
       "category        object\n",
       "categoryCode    object\n",
       "name            object\n",
       "topBrand        object\n",
       "_id.$oid        object\n",
       "cpg.$id.$oid    object\n",
       "cpg.$ref        object\n",
       "brandCode       object\n",
       "dtype: object"
      ]
     },
     "execution_count": 22,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "# Displaying data types of columns in the DataFrame\n",
    "brands_df.dtypes"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 23,
   "id": "78224ac6-e46a-4255-b795-8e8031c1ca45",
   "metadata": {},
   "outputs": [],
   "source": [
    "# Converting 'barcode' to numeric and 'category', 'categoryCode' to categorical format\n",
    "# Converting 'topBrand' to boolean format\n",
    "brands_df['barcode'] = pd.to_numeric(brands_df['barcode'])\n",
    "brands_df['category'] = brands_df.category.astype('category')\n",
    "brands_df['categoryCode'] = brands_df.categoryCode.astype('category')\n",
    "brands_df['topBrand'] = brands_df['topBrand'].astype('bool')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 24,
   "id": "b2f05cc3-62cf-45e8-9b1e-3f357540fb3e",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "barcode           0\n",
       "category        155\n",
       "categoryCode    650\n",
       "name              0\n",
       "topBrand          0\n",
       "_id.$oid          0\n",
       "cpg.$id.$oid      0\n",
       "cpg.$ref          0\n",
       "brandCode       234\n",
       "dtype: int64"
      ]
     },
     "execution_count": 24,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "# Counting missing values in each column of the DataFrame\n",
    "brands_df.isna().sum()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 25,
   "id": "f530647f-596b-41b4-9c2e-9e3310afd48d",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Unique row count for 'category': 23\n",
      "Unique row count for 'categoryCode': 14\n"
     ]
    }
   ],
   "source": [
    "# Counting unique values in 'category' and 'categoryCode' columns\n",
    "unique_category_count = brands_df['category'].nunique()\n",
    "\n",
    "unique_categoryCode_count = brands_df['categoryCode'].nunique()\n",
    "\n",
    "print(\"Unique row count for 'category':\", unique_category_count)\n",
    "print(\"Unique row count for 'categoryCode':\", unique_categoryCode_count)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 26,
   "id": "5a9e9c85-24a5-4780-b360-816552681b84",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Unique category and categoryCode pairs:\n",
      "                         category                   categoryCode\n",
      "0                          Baking                         BAKING\n",
      "1                       Beverages                      BEVERAGES\n",
      "4                  Candy & Sweets               CANDY_AND_SWEETS\n",
      "7             Condiments & Sauces                            NaN\n",
      "8            Canned Goods & Soups                            NaN\n",
      "9                          Baking                            NaN\n",
      "11                            NaN                            NaN\n",
      "13                      Magazines                            NaN\n",
      "14             Breakfast & Cereal                            NaN\n",
      "15              Beer Wine Spirits                            NaN\n",
      "16              Health & Wellness           HEALTHY_AND_WELLNESS\n",
      "19                         Beauty                            NaN\n",
      "20                           Baby                            NaN\n",
      "26                         Frozen                            NaN\n",
      "30                        Grocery                            NaN\n",
      "36                         Snacks                            NaN\n",
      "40                      Household                            NaN\n",
      "41                      Beverages                            NaN\n",
      "52                  Personal Care                            NaN\n",
      "64              Health & Wellness                            NaN\n",
      "68                        Grocery                        GROCERY\n",
      "92                          Dairy                            NaN\n",
      "139                 Personal Care                  PERSONAL_CARE\n",
      "145   Cleaning & Home Improvement  CLEANING_AND_HOME_IMPROVEMENT\n",
      "163                          Deli                            NaN\n",
      "249             Beer Wine Spirits              BEER_WINE_SPIRITS\n",
      "286        Beauty & Personal Care                            NaN\n",
      "298                          Baby                           BABY\n",
      "325                Bread & Bakery               BREAD_AND_BAKERY\n",
      "342                       Outdoor                        OUTDOOR\n",
      "435          Dairy & Refrigerated         DAIRY_AND_REFRIGERATED\n",
      "596                     Magazines                      MAGAZINES\n",
      "1020                       Frozen                         FROZEN\n"
     ]
    }
   ],
   "source": [
    "# Extracting unique pairs of 'category' and 'categoryCode' columns\n",
    "unique_category_pairs = brands_df[['category', 'categoryCode']].drop_duplicates()\n",
    "\n",
    "print(\"Unique category and categoryCode pairs:\")\n",
    "print(unique_category_pairs)"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "6838373c-15d4-4aa6-bd3c-b3356f7d2791",
   "metadata": {},
   "source": [
    "# Users"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 27,
   "id": "026802fe-a2e6-411d-b8a5-9e775d1f56c3",
   "metadata": {},
   "outputs": [],
   "source": [
    "# Converting 'createdDate.$date' and 'lastLogin.$date' columns to datetime format\n",
    "users_df['createdDate.$date'] = pd.to_numeric(users_df['createdDate.$date'])\n",
    "users_df['createdDate.$date']  = users_df['createdDate.$date'].apply(convert_timestamp)\n",
    "\n",
    "users_df['lastLogin.$date'] = pd.to_numeric(users_df['lastLogin.$date'])\n",
    "users_df['lastLogin.$date']  = users_df['lastLogin.$date'].apply(convert_timestamp)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 28,
   "id": "7d1618ca-ce47-4b97-88a1-b094bbd5c8ae",
   "metadata": {},
   "outputs": [],
   "source": [
    "# Converting 'role', 'signUpSource', and 'state' columns to categorical format\n",
    "users_df['role'] = users_df.role.astype('category')\n",
    "users_df['signUpSource'] = users_df.signUpSource.astype('category')\n",
    "users_df['state'] = users_df.state.astype('category')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 29,
   "id": "13e279de-4fee-4752-a674-ba380892d5a0",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/html": [
       "<div>\n",
       "<style scoped>\n",
       "    .dataframe tbody tr th:only-of-type {\n",
       "        vertical-align: middle;\n",
       "    }\n",
       "\n",
       "    .dataframe tbody tr th {\n",
       "        vertical-align: top;\n",
       "    }\n",
       "\n",
       "    .dataframe thead th {\n",
       "        text-align: right;\n",
       "    }\n",
       "</style>\n",
       "<table border=\"1\" class=\"dataframe\">\n",
       "  <thead>\n",
       "    <tr style=\"text-align: right;\">\n",
       "      <th></th>\n",
       "      <th>active</th>\n",
       "      <th>role</th>\n",
       "      <th>signUpSource</th>\n",
       "      <th>state</th>\n",
       "      <th>_id.$oid</th>\n",
       "      <th>createdDate.$date</th>\n",
       "      <th>lastLogin.$date</th>\n",
       "    </tr>\n",
       "  </thead>\n",
       "  <tbody>\n",
       "    <tr>\n",
       "      <th>0</th>\n",
       "      <td>True</td>\n",
       "      <td>consumer</td>\n",
       "      <td>Email</td>\n",
       "      <td>WI</td>\n",
       "      <td>5ff1e194b6a9d73a3a9f1052</td>\n",
       "      <td>2021-01-03 10:24:04</td>\n",
       "      <td>2021-01-03 10:25:37</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>1</th>\n",
       "      <td>True</td>\n",
       "      <td>consumer</td>\n",
       "      <td>Email</td>\n",
       "      <td>WI</td>\n",
       "      <td>5ff1e194b6a9d73a3a9f1052</td>\n",
       "      <td>2021-01-03 10:24:04</td>\n",
       "      <td>2021-01-03 10:25:37</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>2</th>\n",
       "      <td>True</td>\n",
       "      <td>consumer</td>\n",
       "      <td>Email</td>\n",
       "      <td>WI</td>\n",
       "      <td>5ff1e194b6a9d73a3a9f1052</td>\n",
       "      <td>2021-01-03 10:24:04</td>\n",
       "      <td>2021-01-03 10:25:37</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>3</th>\n",
       "      <td>True</td>\n",
       "      <td>consumer</td>\n",
       "      <td>Email</td>\n",
       "      <td>WI</td>\n",
       "      <td>5ff1e1eacfcf6c399c274ae6</td>\n",
       "      <td>2021-01-03 10:25:30</td>\n",
       "      <td>2021-01-03 10:25:30</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>4</th>\n",
       "      <td>True</td>\n",
       "      <td>consumer</td>\n",
       "      <td>Email</td>\n",
       "      <td>WI</td>\n",
       "      <td>5ff1e194b6a9d73a3a9f1052</td>\n",
       "      <td>2021-01-03 10:24:04</td>\n",
       "      <td>2021-01-03 10:25:37</td>\n",
       "    </tr>\n",
       "  </tbody>\n",
       "</table>\n",
       "</div>"
      ],
      "text/plain": [
       "   active      role signUpSource state                  _id.$oid  \\\n",
       "0    True  consumer        Email    WI  5ff1e194b6a9d73a3a9f1052   \n",
       "1    True  consumer        Email    WI  5ff1e194b6a9d73a3a9f1052   \n",
       "2    True  consumer        Email    WI  5ff1e194b6a9d73a3a9f1052   \n",
       "3    True  consumer        Email    WI  5ff1e1eacfcf6c399c274ae6   \n",
       "4    True  consumer        Email    WI  5ff1e194b6a9d73a3a9f1052   \n",
       "\n",
       "     createdDate.$date      lastLogin.$date  \n",
       "0  2021-01-03 10:24:04  2021-01-03 10:25:37  \n",
       "1  2021-01-03 10:24:04  2021-01-03 10:25:37  \n",
       "2  2021-01-03 10:24:04  2021-01-03 10:25:37  \n",
       "3  2021-01-03 10:25:30  2021-01-03 10:25:30  \n",
       "4  2021-01-03 10:24:04  2021-01-03 10:25:37  "
      ]
     },
     "execution_count": 29,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "users_df.head()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 30,
   "id": "5f83ec1c-e10b-4e85-a543-8904146dde95",
   "metadata": {
    "tags": []
   },
   "outputs": [
    {
     "data": {
      "text/plain": [
       "(495, 7)"
      ]
     },
     "execution_count": 30,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "# Number of rows and columns of the DataFrame\n",
    "users_df.shape"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 31,
   "id": "c5b93795-ac49-496c-9c13-e55191da12fe",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/html": [
       "<div>\n",
       "<style scoped>\n",
       "    .dataframe tbody tr th:only-of-type {\n",
       "        vertical-align: middle;\n",
       "    }\n",
       "\n",
       "    .dataframe tbody tr th {\n",
       "        vertical-align: top;\n",
       "    }\n",
       "\n",
       "    .dataframe thead th {\n",
       "        text-align: right;\n",
       "    }\n",
       "</style>\n",
       "<table border=\"1\" class=\"dataframe\">\n",
       "  <thead>\n",
       "    <tr style=\"text-align: right;\">\n",
       "      <th></th>\n",
       "      <th>active</th>\n",
       "      <th>role</th>\n",
       "      <th>signUpSource</th>\n",
       "      <th>state</th>\n",
       "      <th>_id.$oid</th>\n",
       "      <th>createdDate.$date</th>\n",
       "      <th>lastLogin.$date</th>\n",
       "    </tr>\n",
       "  </thead>\n",
       "  <tbody>\n",
       "    <tr>\n",
       "      <th>count</th>\n",
       "      <td>495</td>\n",
       "      <td>495</td>\n",
       "      <td>447</td>\n",
       "      <td>439</td>\n",
       "      <td>495</td>\n",
       "      <td>495</td>\n",
       "      <td>433</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>unique</th>\n",
       "      <td>2</td>\n",
       "      <td>2</td>\n",
       "      <td>2</td>\n",
       "      <td>8</td>\n",
       "      <td>212</td>\n",
       "      <td>212</td>\n",
       "      <td>171</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>top</th>\n",
       "      <td>True</td>\n",
       "      <td>consumer</td>\n",
       "      <td>Email</td>\n",
       "      <td>WI</td>\n",
       "      <td>54943462e4b07e684157a532</td>\n",
       "      <td>2014-12-19 09:21:22</td>\n",
       "      <td>2021-03-05 11:52:23</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>freq</th>\n",
       "      <td>494</td>\n",
       "      <td>413</td>\n",
       "      <td>443</td>\n",
       "      <td>396</td>\n",
       "      <td>20</td>\n",
       "      <td>20</td>\n",
       "      <td>20</td>\n",
       "    </tr>\n",
       "  </tbody>\n",
       "</table>\n",
       "</div>"
      ],
      "text/plain": [
       "       active      role signUpSource state                  _id.$oid  \\\n",
       "count     495       495          447   439                       495   \n",
       "unique      2         2            2     8                       212   \n",
       "top      True  consumer        Email    WI  54943462e4b07e684157a532   \n",
       "freq      494       413          443   396                        20   \n",
       "\n",
       "          createdDate.$date      lastLogin.$date  \n",
       "count                   495                  433  \n",
       "unique                  212                  171  \n",
       "top     2014-12-19 09:21:22  2021-03-05 11:52:23  \n",
       "freq                     20                   20  "
      ]
     },
     "execution_count": 31,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "# Generating descriptive statistics for the DataFrame\n",
    "users_df.describe()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 32,
   "id": "821ddca3-45f3-4eb3-9c0f-127c4d54b2ba",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "active                   bool\n",
       "role                 category\n",
       "signUpSource         category\n",
       "state                category\n",
       "_id.$oid               object\n",
       "createdDate.$date      object\n",
       "lastLogin.$date        object\n",
       "dtype: object"
      ]
     },
     "execution_count": 32,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "# Displaying data types of columns in the DataFrame\n",
    "users_df.dtypes"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 33,
   "id": "76183d52-3b9e-43dd-82c3-a334b56ee41c",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "active                0\n",
       "role                  0\n",
       "signUpSource         48\n",
       "state                56\n",
       "_id.$oid              0\n",
       "createdDate.$date     0\n",
       "lastLogin.$date      62\n",
       "dtype: int64"
      ]
     },
     "execution_count": 33,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "# Counting missing values in each column of the DataFrame\n",
    "users_df.isna().sum()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 34,
   "id": "5d371008-9186-4058-8aa4-b8f6dfc9ce9f",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "395\n"
     ]
    }
   ],
   "source": [
    "# Calculating the count where last login date is missing or greater than created date\n",
    "createdDate_lastLogin_count = len(users_df[\n",
    "    (users_df['lastLogin.$date'].isnull()) |\n",
    "    (users_df['lastLogin.$date'] > users_df['createdDate.$date'])\n",
    "])\n",
    "\n",
    "print(createdDate_lastLogin_count)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 35,
   "id": "a8a3f058-3fd5-49d8-826b-eb165d5d821e",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Unique values in the 'state' column:\n",
      "['WI', 'KY', 'AL', 'CO', 'IL', NaN, 'OH', 'SC', 'NH']\n",
      "Categories (8, object): ['AL', 'CO', 'IL', 'KY', 'NH', 'OH', 'SC', 'WI']\n"
     ]
    }
   ],
   "source": [
    "# Extracting unique values from the 'state' column\n",
    "unique_states = users_df['state'].unique()\n",
    "\n",
    "print(\"Unique values in the 'state' column:\")\n",
    "print(unique_states)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 36,
   "id": "cb0dd483-9f66-448d-b39d-3bdd588c2b9c",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Unique values in the 'role' column:\n",
      "['consumer', 'fetch-staff']\n",
      "Categories (2, object): ['consumer', 'fetch-staff']\n"
     ]
    }
   ],
   "source": [
    "# Extracting unique values from the 'role' column\n",
    "unique_role = users_df['role'].unique()\n",
    "\n",
    "print(\"Unique values in the 'role' column:\")\n",
    "print(unique_role)"
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3 (ipykernel)",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.9.13"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
