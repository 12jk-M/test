from __future__ import unicode_literals

from django.db import models
from django.db.models.signals import post_save
from django.conf import settings

# Create your models here.
class Qymltable(models.Model):
	dwmc = models.CharField(max_length=100)
	dz = models.CharField(max_length=100,blank=True)
	fzr = models.CharField(max_length=60,blank=True)
	tel = models.CharField(max_length=100,blank=True)
	fax = models.CharField(max_length=100,blank=True)
	youbian = models.CharField(max_length=6,blank=True)
	email = models.CharField(max_length=30,blank=True)
	www = models.CharField(max_length=50,blank=True)
	gx = models.CharField(max_length=30,blank=True)
	
	def __unicode__(self):
		return u"%s" %(self.dwmc)
	