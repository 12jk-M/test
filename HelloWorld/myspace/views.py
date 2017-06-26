# -*- coding: UTF-8 -*-
# from django.shortcuts import render
from django.shortcuts import render
# from django.shortcuts import render_to_response
from django.http import HttpResponse, HttpResponseRedirect
#from qyml.forms import QymlForm, SignupForm
from models import Qymltable
from django.shortcuts import get_object_or_404
from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger, InvalidPage
#from django.contrib.auth import authenticate, login
from django.contrib.auth import authenticate, login as user_login, logout as user_logout
from django.views.decorators.csrf import csrf_protect
from django.template import RequestContext

def list(request):
	contacts = Qymltable.objects.all()
	return render(request,'list.html',{"contacts":contacts})