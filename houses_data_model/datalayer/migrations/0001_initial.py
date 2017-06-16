# -*- coding: utf-8 -*-
# Generated by Django 1.11 on 2017-06-06 12:12
from __future__ import unicode_literals

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='Adjacent_Suburbs',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('id_suburb_adjacent', models.IntegerField()),
                ('row_insert_date', models.DateTimeField()),
            ],
            options={
                'db_table': 'adjacent_suburbs',
            },
        ),
        migrations.CreateModel(
            name='Agency',
            fields=[
                ('id_agency', models.AutoField(primary_key=True, serialize=False)),
                ('id_agency_tm', models.IntegerField(null=True)),
                ('name', models.CharField(max_length=100, null=True)),
                ('fax', models.CharField(max_length=30, null=True)),
                ('is_licenced_property_agency', models.SmallIntegerField(null=True)),
                ('is_real_estate_agency', models.SmallIntegerField(null=True)),
                ('phone', models.CharField(max_length=30, null=True)),
                ('website', models.CharField(max_length=255, null=True)),
                ('logo1', models.CharField(max_length=255, null=True)),
                ('logo2', models.CharField(max_length=255, null=True)),
                ('office_location', models.CharField(max_length=255, null=True)),
                ('row_insert_date', models.DateTimeField()),
            ],
            options={
                'db_table': 'agency',
            },
        ),
        migrations.CreateModel(
            name='Agent_Rental',
            fields=[
                ('id_agent_rental', models.AutoField(primary_key=True, serialize=False)),
                ('id_rental_listing', models.IntegerField(null=True)),
                ('fullname', models.CharField(max_length=50, null=True)),
                ('mobile', models.CharField(max_length=20, null=True)),
                ('office_phone', models.CharField(max_length=20, null=True)),
                ('photo', models.URLField(null=True)),
                ('url_slug', models.CharField(max_length=50, null=True)),
                ('row_insert_date', models.DateTimeField()),
            ],
            options={
                'db_table': 'agent_rental',
            },
        ),
        migrations.CreateModel(
            name='District',
            fields=[
                ('id_district', models.AutoField(primary_key=True, serialize=False)),
                ('id_district_tm', models.IntegerField()),
                ('name', models.CharField(max_length=40)),
                ('row_insert_date', models.DateTimeField()),
            ],
            options={
                'db_table': 'district',
            },
        ),
        migrations.CreateModel(
            name='Location',
            fields=[
                ('id_location', models.AutoField(primary_key=True, serialize=False)),
                ('row_insert_date', models.DateTimeField()),
                ('id_district', models.ForeignKey(db_column='id_district', on_delete=django.db.models.deletion.CASCADE, to='datalayer.District')),
            ],
            options={
                'db_table': 'location',
            },
        ),
        migrations.CreateModel(
            name='Region',
            fields=[
                ('id_region', models.AutoField(primary_key=True, serialize=False)),
                ('id_region_tm', models.IntegerField()),
                ('name', models.CharField(max_length=40)),
                ('row_insert_date', models.DateTimeField()),
            ],
            options={
                'db_table': 'region',
            },
        ),
        migrations.CreateModel(
            name='Rental_Listings',
            fields=[
                ('id_rental_listing', models.AutoField(primary_key=True, serialize=False)),
                ('id_listing_tm', models.IntegerField()),
                ('amenities', models.TextField(null=True)),
                ('available_from', models.CharField(max_length=255, null=True)),
                ('bathrooms', models.PositiveSmallIntegerField(null=True)),
                ('bedrooms', models.PositiveSmallIntegerField(null=True)),
                ('best_contact_time', models.CharField(max_length=255, null=True)),
                ('end_date', models.DateTimeField(null=True)),
                ('ideal_tenant', models.TextField(null=True)),
                ('listing_group', models.CharField(max_length=30, null=True)),
                ('max_tenants', models.SmallIntegerField(null=True)),
                ('parking', models.TextField(null=True)),
                ('pets_okay', models.SmallIntegerField(null=True)),
                ('price_display', models.CharField(max_length=30, null=True)),
                ('property_id', models.CharField(max_length=20, null=True)),
                ('property_type', models.CharField(max_length=20, null=True)),
                ('smokers_okay', models.SmallIntegerField(null=True)),
                ('start_date', models.DateTimeField(null=True)),
                ('start_price', models.DecimalField(decimal_places=2, max_digits=11, null=True)),
                ('title', models.CharField(max_length=100, null=True)),
                ('rent_per_week', models.DecimalField(decimal_places=2, max_digits=11, null=True)),
                ('whiteware', models.CharField(max_length=255, null=True)),
                ('id_suburb_tm', models.IntegerField(db_column='id_suburb_tm')),
                ('address', models.CharField(max_length=255, null=True)),
                ('latitude', models.CharField(max_length=20)),
                ('longitude', models.CharField(max_length=20)),
                ('easting', models.CharField(max_length=20)),
                ('northing', models.CharField(max_length=20)),
                ('agency_reference', models.CharField(max_length=30)),
                ('category', models.CharField(max_length=30, null=True)),
                ('category_path', models.CharField(max_length=100, null=True)),
                ('has_embedded_video', models.SmallIntegerField(null=True)),
                ('has_gallery', models.SmallIntegerField(null=True)),
                ('is_bold', models.SmallIntegerField(null=True)),
                ('is_featured', models.SmallIntegerField(null=True)),
                ('is_highlighted', models.SmallIntegerField(null=True)),
                ('is_super_featured', models.SmallIntegerField(null=True)),
                ('listing_length', models.CharField(max_length=20, null=True)),
                ('note_date', models.DateTimeField(null=True)),
                ('reserve_state', models.SmallIntegerField(null=True)),
                ('photo_main', models.CharField(max_length=255, null=True)),
                ('is_boosted', models.SmallIntegerField(null=True)),
                ('is_current', models.SmallIntegerField(null=True)),
                ('data_source', models.CharField(max_length=20)),
                ('valid_from', models.DateTimeField()),
                ('valid_to', models.DateTimeField()),
                ('row_insert_date', models.DateTimeField()),
                ('id_agency_tm', models.ForeignKey(db_column='id_agency_tm', null=True, on_delete=django.db.models.deletion.CASCADE, to='datalayer.Agency')),
            ],
            options={
                'db_table': 'rental_listings',
            },
        ),
        migrations.CreateModel(
            name='Rental_Photo',
            fields=[
                ('id_rental_photo', models.AutoField(primary_key=True, serialize=False)),
                ('photo_url', models.CharField(max_length=255)),
                ('row_insert_date', models.DateTimeField()),
                ('id_rental_listing', models.ForeignKey(db_column='id_rental_listing', on_delete=django.db.models.deletion.CASCADE, to='datalayer.Rental_Listings')),
            ],
            options={
                'db_table': 'rental_photo',
            },
        ),
        migrations.CreateModel(
            name='Suburb',
            fields=[
                ('id_suburb', models.AutoField(primary_key=True, serialize=False)),
                ('id_suburb_tm', models.IntegerField()),
                ('name', models.CharField(max_length=40)),
                ('row_insert_date', models.DateTimeField()),
            ],
            options={
                'db_table': 'suburb',
            },
        ),
        migrations.AddField(
            model_name='location',
            name='id_region',
            field=models.ForeignKey(db_column='id_region', on_delete=django.db.models.deletion.CASCADE, to='datalayer.Region'),
        ),
        migrations.AddField(
            model_name='location',
            name='id_suburb',
            field=models.ForeignKey(db_column='id_suburb', on_delete=django.db.models.deletion.CASCADE, to='datalayer.Suburb'),
        ),
        migrations.AddField(
            model_name='adjacent_suburbs',
            name='id_suburb',
            field=models.ForeignKey(db_column='id_suburb', on_delete=django.db.models.deletion.CASCADE, to='datalayer.Suburb'),
        ),
    ]