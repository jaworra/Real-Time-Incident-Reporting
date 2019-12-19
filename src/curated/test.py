infeature="Duplic"
lista=[]
field="uniqID"
cursor1=arcpy.SearchCursor(infeature)
for row in cursor1:
    i=row.getValue(field)    
    lista.append(i)
del cursor1, row

def duplicates(field_in):        
    occ=lista.count(field_in)
    return occ

Duplic =
duplicates(!UniqID!)
