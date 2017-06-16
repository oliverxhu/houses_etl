from django.db import models

# Create your models here.

class Agency(models.Model):  # SCD type 1
    class Meta:
        db_table = 'agency'
    id_agency = models.AutoField(primary_key=True)
    id_agency_tm = models.IntegerField(null=True)
    name = models.CharField(max_length=255, null=True)
    fax = models.CharField(max_length=255, null=True)
    is_licenced_property_agency = models.NullBooleanField()
    is_real_estate_agency = models.NullBooleanField()
    phone = models.CharField(max_length=255, null=True)
    website = models.CharField(max_length=255, null=True)
    logo1 = models.CharField(max_length=255, null=True)
    logo2 = models.CharField(max_length=255, null=True)
    office_location = models.CharField(max_length=255, null=True)
    row_insert_date = models.DateTimeField()

class Rental_Agent(models.Model):
    class Meta:
        db_table = 'rental_agent'
    id_agent_rental = models.AutoField(primary_key=True)
    id_rental_listing = models.IntegerField(null=True)  # field we join on
    fullname = models.CharField(max_length=255, null=True)
    mobile = models.CharField(max_length=255, null=True)
    office_phone = models.CharField(max_length=255, null=True)
    photo = models.URLField(null=True)
    url_slug = models.CharField(max_length=255, null=True)
    row_insert_date = models.DateTimeField()

class Rental_Listings(models.Model):
    class Meta:
        db_table = 'rental_listings'
    id_rental_listing = models.AutoField(primary_key=True)
    id_listing_tm = models.IntegerField()
    id_agency_tm = models.IntegerField(db_column='id_agency_tm', null=True)
    amenities = models.TextField(null=True)
    available_from = models.CharField(max_length=255, null=True)
    bathrooms = models.PositiveSmallIntegerField(null=True)
    bedrooms = models.PositiveSmallIntegerField(null=True)
    best_contact_time = models.CharField(max_length=255, null=True)
    end_date = models.DateTimeField(null=True)
    ideal_tenant = models.TextField(null=True)
    listing_group = models.CharField(max_length=255, null=True)
    max_tenants = models.SmallIntegerField(null=True)
    parking = models.TextField(null=True)
    pets_okay = models.SmallIntegerField(null=True)
    price_display = models.CharField(max_length=255, null=True)
    property_id = models.CharField(max_length=255, null=True)
    property_type = models.CharField(max_length=255, null=True)
    smokers_okay = models.SmallIntegerField(null=True)
    start_date = models.DateTimeField(null=True)
    start_price = models.IntegerField(null=True)
    title = models.CharField(max_length=255, null=True)
    rent_per_week = models.IntegerField(null=True)
    whiteware = models.CharField(max_length=255, null=True)    
    id_suburb_tm = models.IntegerField(db_column='id_suburb_tm')
    address = models.CharField(max_length=255, null=True)
    latitude = models.FloatField(null=True)
    longitude = models.FloatField(null=True)
    easting = models.FloatField(null=True)
    northing = models.FloatField(null=True)
    agency_reference = models.CharField(max_length=255, null=True)
    category = models.CharField(max_length=255, null=True)
    category_path = models.CharField(max_length=255, null=True)
    has_embedded_video = models.NullBooleanField()
    has_gallery = models.NullBooleanField()
    is_bold = models.NullBooleanField()
    is_classified = models.NullBooleanField()
    is_featured = models.NullBooleanField()
    is_highlighted = models.NullBooleanField()
    is_super_featured = models.NullBooleanField()
    listing_length = models.CharField(max_length=255, null=True)  # what is this?
    note_date = models.CharField(max_length=255, null=True)
    reserve_state = models.SmallIntegerField(null=True)
    photo_main = models.CharField(max_length=255, null=True)
    is_boosted = models.NullBooleanField()
    is_current = models.SmallIntegerField(null=True)
    valid_from = models.DateTimeField()
    valid_to = models.DateTimeField()
    row_insert_date = models.DateTimeField()


class Region(models.Model):  # SCD 1
    class Meta:
        db_table = 'region'
    id_region = models.AutoField(primary_key=True)
    id_region_tm = models.IntegerField()
    name = models.CharField(max_length=255)
    row_insert_date = models.DateTimeField()

class District(models.Model):  # SCD 1
    class Meta:
        db_table = 'district'
    id_district = models.AutoField(primary_key=True)
    id_district_tm = models.IntegerField()
    name = models.CharField(max_length=255)
    row_insert_date = models.DateTimeField()

class Suburb(models.Model):  # SCD 1
    class Meta:
        db_table = 'suburb'
    id_suburb = models.AutoField(primary_key=True)
    id_suburb_tm = models.IntegerField()
    name = models.CharField(max_length=255)
    row_insert_date = models.DateTimeField()

class Location(models.Model):
    class Meta:
        db_table = 'location'
    id_location = models.AutoField(primary_key=True)
    id_region = models.ForeignKey(Region, db_column='id_region')
    id_district = models.ForeignKey(District, db_column='id_district')
    id_suburb = models.ForeignKey(Suburb, db_column='id_suburb')
    row_insert_date = models.DateTimeField()

class Adjacent_Suburbs(models.Model):
    class Meta:
        db_table = 'adjacent_suburbs'
    id_suburb = models.ForeignKey(Suburb, db_column='id_suburb')
    id_suburb_adjacent = models.IntegerField()
    row_insert_date = models.DateTimeField()


class Rental_Photo(models.Model):
    class Meta:
        db_table = 'rental_photo'
    id_rental_photo = models.AutoField(primary_key=True)
    id_rental_listing = models.IntegerField(db_column='id_rental_listing')
    photo_url = models.CharField(max_length=255)
    row_insert_date = models.DateTimeField()
