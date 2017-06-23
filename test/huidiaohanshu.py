def sayhello(words):
	if isinstance(words,str):
		print words
	else:
		raise ValueError('error')
		
def man(callback,words):
	callback(words)
	
if __name__=='__main__':
	man(sayhello,'nihao')