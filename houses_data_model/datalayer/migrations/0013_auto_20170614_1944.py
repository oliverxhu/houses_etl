# -*- coding: utf-8 -*-
# Generated by Django 1.11 on 2017-06-14 09:44
from __future__ import unicode_literals

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('datalayer', '0012_auto_20170613_2236'),
    ]

    operations = [
        migrations.RenameModel(
            old_name='Agent_Rental',
            new_name='Rental_Agent',
        ),
        migrations.AlterModelTable(
            name='rental_agent',
            table='rental_agent',
        ),
    ]
