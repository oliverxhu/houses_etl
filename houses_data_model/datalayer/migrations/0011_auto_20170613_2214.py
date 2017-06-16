# -*- coding: utf-8 -*-
# Generated by Django 1.11 on 2017-06-13 12:14
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('datalayer', '0010_auto_20170607_1150'),
    ]

    operations = [
        migrations.AlterField(
            model_name='rental_listings',
            name='easting',
            field=models.FloatField(null=True),
        ),
        migrations.AlterField(
            model_name='rental_listings',
            name='latitude',
            field=models.FloatField(null=True),
        ),
        migrations.AlterField(
            model_name='rental_listings',
            name='longitude',
            field=models.FloatField(null=True),
        ),
        migrations.AlterField(
            model_name='rental_listings',
            name='northing',
            field=models.FloatField(null=True),
        ),
    ]