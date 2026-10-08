#!/usr/bin/env python3
"""Export Poland automotive POIs from real OpenStreetMap PBF, never synthetic data."""
import argparse, collections, csv, datetime, json, math, os, sqlite3, sys
from pathlib import Path
import osmium

CATEGORIES=[
 ('fuel',[('amenity','fuel')]),('charging_station',[('amenity','charging_station')]),
 ('car_wash',[('amenity','car_wash')]),('vehicle_inspection',[('amenity','vehicle_inspection'),('shop','vehicle_inspection')]),
 ('driving_school',[('amenity','driving_school'),('office','driving_school')]),
 ('car_rental',[('amenity','car_rental'),('shop','car_rental')]),('car_sharing',[('amenity','car_sharing')]),
 ('car_dealership',[('shop','car')]),('truck_dealership',[('shop','truck')]),
 ('motorcycle_shop',[('shop','motorcycle')]),('car_parts',[('shop','car_parts'),('shop','car_accessories')]),
 ('tyres',[('shop','tyres'),('shop','tire'),('shop','tyre')]),
 ('car_dismantling',[('shop','car_scrap'),('shop','car_wreck'),('recycling:cars','yes')]),
 ('auto_detailing',[('shop','car_detailing'),('craft','car_detailing')]),
 ('road_assistance',[('office','towing'),('shop','towing')]),
 ('car_repair',[('shop','car_repair'),('craft','car_repair'),('shop','auto_repair')])
]
SUBTYPES={'service:vehicle:tyres':'tyres','service:vehicle:inspection':'vehicle_inspection','service:vehicle:glass':'car_glass',
 'service:vehicle:body_repair':'body_shop','service:vehicle:detailing':'auto_detailing','service:vehicle:towing':'road_assistance',
 'service:vehicle:electrical':'auto_electrician','service:vehicle:oil_change':'oil_service',
 'service:vehicle:air_conditioning':'air_conditioning','service:vehicle:paint':'body_shop'}
HEAD=['source','source_id','osm_type','name','category','lat','lng','city','address','phone','website','country_code','subcategory',
 'categories_all','postcode','brand','operator','opening_hours','source_url','data_source','review_status','quality_flags']
def string(x,n=255): return ' '.join(str(x or '').split())[:n]
def match(t):
 if any(t.get(q) in ('yes','true') for q in ('disused','abandoned','demolished','proposed','construction')):return []
 if any(k.startswith(('disused:','abandoned:','was:','demolished:','proposed:')) for k in t):return []
 cats=[c for c,rules in CATEGORIES if any(t.get(k)==v for k,v in rules)]
 if 'car_repair' in cats or 'tyres' in cats:
  cats.extend(v for k,v in SUBTYPES.items() if t.get(k) in ('yes','only','designated') and v not in cats)
 return cats
def convert(kind,oid,t,lat,lon):
 cats=match(t)
 if not cats:return None
 name=string(t.get('name') or t.get('name:pl') or t.get('brand'),180)
 if not name or name.lower() in ('todo','fixme','no name','unnamed','brak nazwy'):return None
 if not (math.isfinite(lat) and math.isfinite(lon) and 48.9<=lat<=55 and 13.9<=lon<=24.3):return None
 country=string(t.get('addr:country')).upper()
 if country and country not in ('PL','POL','POLAND','POLSKA'):return None
 city=string(t.get('addr:city') or t.get('is_in:city') or t.get('addr:municipality') or t.get('addr:place'))
 street=' '.join(filter(None,[string(t.get('addr:street') or t.get('addr:place')),string(t.get('addr:housenumber'))]))
 address=', '.join(filter(None,[street,' '.join(filter(None,[string(t.get('addr:postcode')),city]))]))
 phone=string(t.get('contact:phone') or t.get('phone'),100)
 website=string(t.get('contact:website') or t.get('website') or t.get('url'),500)
 if website.startswith('www.'):website='https://'+website
 if website and not website.startswith(('http://','https://')):website=''
 flags=[]
 if not city:flags.append('NO_CITY')
 if not street:flags.append('NO_STREET_ADDRESS')
 if not phone and not website:flags.append('NO_CONTACT')
 if len(cats)>1:flags.append('MULTI_CATEGORY')
 return dict(source='osm',source_id=str(oid),osm_type=kind,name=name,category=cats[0],
  lat=f'{lat:.7f}',lng=f'{lon:.7f}',city=city,address=address,phone=phone,website=website,
  country_code='PL',subcategory='|'.join(cats[1:]),categories_all='|'.join(cats),
  postcode=string(t.get('addr:postcode')),brand=string(t.get('brand')),operator=string(t.get('operator')),
  opening_hours=string(t.get('opening_hours'),1000),source_url=f'https://www.openstreetmap.org/{kind}/{oid}',
  data_source='OpenStreetMap Geofabrik Poland',review_status='UNVERIFIED',quality_flags='|'.join(flags))
class Catalogue:
 def __init__(self,out):
  self.out=out;out.mkdir(parents=True,exist_ok=True)
  self.db=sqlite3.connect(str(out/'poi_staging.sqlite3'))
  self.db.execute('CREATE TABLE IF NOT EXISTS poi (key TEXT PRIMARY KEY,category TEXT,name TEXT,payload TEXT)')
  self.total=0
 def save(self,kind,oid,t,lat,lon):
  r=convert(kind,oid,t,lat,lon)
  if not r:return
  self.db.execute('INSERT OR REPLACE INTO poi VALUES (?,?,?,?)',(f'{kind}:{oid}',r['category'],r['name'],json.dumps(r,ensure_ascii=False)))
  self.total+=1
  if self.total%10000==0:self.db.commit();print('Found',self.total,'POIs',flush=True)
 def export(self,chunk_size):
  self.db.commit()
  allf=self.out/'auto_poi_PL_ALL_REVIEW.csv'
  batchdir=self.out/'IMPORT_DRAFT_BATCHES';batchdir.mkdir(exist_ok=True)
  counts=collections.Counter();chunks=0;write=None;batch=None;records=0
  with allf.open('w',encoding='utf-8-sig',newline='') as f:
   writer=csv.DictWriter(f,fieldnames=HEAD);writer.writeheader()
   try:
    for (payload,) in self.db.execute('SELECT payload FROM poi ORDER BY category,name,key'):
     r=json.loads(payload);writer.writerow(r);counts[r['category']]+=1
     if records%chunk_size==0:
      if batch:batch.close()
      chunks+=1;batch=(batchdir/f'autosluga_poi_pl_{chunks:04d}.csv').open('w',encoding='utf-8-sig',newline='')
      write=csv.DictWriter(batch,fieldnames=HEAD);write.writeheader()
     write.writerow(r);records+=1
   finally:
    if batch:batch.close()
  summary={'unique_osm_pois':records,'batch_count':chunks,'by_primary_category':dict(counts),
   'generated_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),
   'data_source':'Geofabrik Poland / OpenStreetMap','review_status':'UNVERIFIED',
   'license':'ODbL 1.0, © OpenStreetMap contributors'}
  (self.out/'SUMMARY.json').write_text(json.dumps(summary,indent=2,ensure_ascii=False),encoding='utf-8')
  with (self.out/'CATEGORY_COUNTS.csv').open('w',encoding='utf-8-sig',newline='') as f:
   w=csv.writer(f);w.writerow(['category','count']);w.writerows(sorted(counts.items()))
  print('EXPORTED',records,'ROWS',chunks,'BATCHES',flush=True)
def main():
 p=argparse.ArgumentParser();p.add_argument('--input',required=True);p.add_argument('--output',required=True)
 p.add_argument('--chunk-size',type=int,default=1000);a=p.parse_args()
 if not 1<=a.chunk_size<=5000:p.error('chunk-size must be 1..5000')
 out=Catalogue(Path(a.output))
 class Handler(osmium.SimpleHandler):
  def node(self,n):
   if not n.tags or not n.location.valid():return
   t=dict(n.tags)
   if match(t):out.save('node',n.id,t,n.location.lat,n.location.lon)
  def way(self,w):
   if not w.tags:return
   t=dict(w.tags)
   if not match(t):return
   coords=[(n.location.lat,n.location.lon) for n in w.nodes if n.location.valid()]
   if coords:out.save('way',w.id,t,sum(x for x,_ in coords)/len(coords),sum(y for _,y in coords)/len(coords))
  def area(self,a):
   if a.from_way() or not a.tags:return
   t=dict(a.tags)
   if not match(t):return
   ring=next(iter(a.outer_rings()),None)
   if ring is None:return
   coords=[(n.lat,n.lon) for n in ring if n.location.valid()]
   if coords:out.save('relation',a.orig_id(),t,sum(x for x,_ in coords)/len(coords),sum(y for _,y in coords)/len(coords))
 Handler().apply_file(a.input,locations=True,idx='sparse_file_array')
 out.export(a.chunk_size)
if __name__=='__main__':main()
