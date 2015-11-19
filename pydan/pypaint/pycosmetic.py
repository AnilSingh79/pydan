'''
Created on Nov 19, 2015

@author: Anil Singh
'''

class pyaxiscosmetics(object):
    pass

class pycanvascosmetics(object):
    pass

class pyplotcosmetics(object):
    pass

class pycosmetics(object):
    def __init__(self):
        self.label = ''
        self.style = 'step'
        self.lineWidth= -1
        self.color='black'
        self.edgeColor='black'
        self.hatch = ''
        self.alpha = 0.5
        self.xlabel = ''
        self.ylabel = ''
    
        self.xMin = 0
        self.xMax = 0
        self.yMin = 0
        self.yMax = 100
    
    def set_label(self, label):
        self.label = label
    def set_style(self,style):
        self.style  = style
    def set_linewidth(self,wid):
        self.lineWidth = wid
    def set_color(self,color):
        self.color =color
    def set_line_color(self,color):
        self.edgeColor = color
    def set_hatch(self,hatch):
        self.hatch = hatch    
    def set_xlabel(self,x):
        self.xlabel = x
    def set_ylabel(self,y):
        self.ylabel = y
    