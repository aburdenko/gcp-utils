import json
import functions_framework
from flask import escape

import pandas as pd
import useful_rdkit_utils as uru
from rdkit import Chem

@functions_framework.http
def fingerprint(request):
  request_json = request.get_json(silent=True)
  replies = []
  smiles = request_json['smiles']
  for smile_list in smiles:
    smile = smile_list[0]
    mol = Chem.MolFromSmiles(smile)
    fp = uru.mol2morgan_fp(mol)

    replies.append({
      'smile': {smile},
      'fingerprint':  {fp}
    })
  return json.dumps({
    # each reply is a STRING (JSON not currently supported)
    'replies': [json.dumps(reply) for reply in replies]
  })
  
@functions_framework.http
def add_fake_user(request):
      request_json = request.get_json(silent=True)
      replies = []
      calls = request_json['calls']
      for call in calls:
        userno = call[0]
        corp = call[1]
        replies.append({
          'username': f'user_{userno}',
          'email': f'user_{userno}@{corp}.com'
        })
      return json.dumps({
        # each reply is a STRING (JSON not currently supported)
        'replies': [json.dumps(reply) for reply in replies]
      })