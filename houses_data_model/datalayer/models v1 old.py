from django.db import models

# Create your models here.

class Agency(models.Model):
    class Meta:
        db_table = 'agency'
    id_agency = models.AutoField(primary_key=True)
    id_agency_tm = models.IntegerField(null=True)
    name = models.CharField(max_length=100, null=True)
    fax = models.CharField(max_length=30, null=True)
    is_licenced_property = models.NullBooleanField()
    is_real_estate = models.NullBooleanField()
    phone = models.CharField(max_length=30, null=True)
    website = models.URLField(null=True)
    last_updated = models.DateTimeField()

class Agent(models.Model):
    class Meta:
        db_table = 'agent'
    id_agent = models.AutoField(primary_key=True)
    fullname = models.CharField(max_length=50, null=True)
    mobile = models.CharField(max_length=20, null=True)
    office_phone = models.CharField(max_length=20, null=True)
    photo = models.URLField(null=True)
    url_slug = models.CharField(max_length=50, null=True)
    last_updated = models.DateTimeField()

class Rental_Listings(models.Model):
    class Meta:
        db_table = 'rental_listings'
    id_rental_listing = models.AutoField(primary_key=True)
    id_listing_tm = models.IntegerField()
    listing_type = models.CharField(max_length=20)
    id_agency = models.ForeignKey(Agency, null=True, db_column='id_agency')
    agent = models.ManyToManyField(Agent, through='Agent_Rental_Detail', through_fields=('id_rental_listing', 'id_agent'))
    is_current = models.NullBooleanField()
    data_source = models.CharField(max_length=20)
    job_run_datetime = models.DateTimeField()
    valid_from = models.DateTimeField()
    valid_to = models.DateTimeField()
    last_updated = models.DateTimeField()
    version = models.IntegerField()

class Residential_Listings(models.Model):
    class Meta:
        db_table = 'residential_listings'
    id_residential_listing = models.AutoField(primary_key=True)
    id_listing_tm = models.IntegerField()
    listing_type = models.CharField(max_length=20)
    id_agency = models.ForeignKey(Agency, null=True, db_column='id_agency')
    agent = models.ManyToManyField(Agent, through='Agent_Residential_Detail', through_fields=('id_residential_listing', 'id_agent'))
    is_current = models.NullBooleanField()
    data_source = models.CharField(max_length=20)
    job_run_datetime = models.DateTimeField()
    valid_from = models.DateTimeField()
    valid_to = models.DateTimeField()
    last_updated = models.DateTimeField()
    version = models.IntegerField()

class Rental_History(models.Model):
    class Meta:
        db_table = 'rental_history'
    id_rental_listing = models.IntegerField(primary_key=True, db_column='id_rental_listing')
    id_rental_listing_tm = models.IntegerField()
    valid_from = models.DateTimeField()
    valid_to = models.DateTimeField()
    last_updated = models.DateTimeField()

class Residential_History(models.Model):
    class Meta:
        db_table = 'residential_history'
    id_residential_listing = models.IntegerField(primary_key=True, db_column='id_residential_listing')
    id_residential_listing_tm = models.IntegerField()
    valid_from = models.DateTimeField()
    valid_to = models.DateTimeField()
    last_updated = models.DateTimeField()

class Detail_Main_Rental(models.Model):
    class Meta:
        db_table = 'detail_main_rental'
    id_rental_listing = models.OneToOneField(Rental_Listings, primary_key=True, db_column='id_rental_listing')
    amenities = models.TextField(null=True)
    available_from = models.DateTimeField(null=True)
    bathrooms = models.PositiveSmallIntegerField(null=True)
    bedrooms = models.PositiveSmallIntegerField(null=True)
    best_contact_time = models.DateTimeField(null=True)
    end_date = models.DateTimeField(null=True)
    ideal_tenant = models.TextField(null=True)
    listing_group = models.CharField(max_length=30, null=True)
    max_tenants = models.SmallIntegerField(null=True)
    parking = models.TextField(null=True)
    pets_okay = models.NullBooleanField()
    price_display = models.CharField(max_length=30, null=True)
    property_id = models.CharField(max_length=20, null=True)
    property_type = models.CharField(max_length=20, null=True)
    smokers_okay = models.NullBooleanField()
    start_date = models.DateTimeField(null=True)
    start_price = models.DecimalField(max_digits=11, decimal_places=2, null=True)
    title = models.CharField(max_length=100, null=True)
    rent_per_week = models.DecimalField(max_digits=11, decimal_places=2, null=True)
    whiteware = models.CharField(max_length=255, null=True)
    last_updated = models.DateTimeField()

class Detail_Main_Residential(models.Model):
    class Meta:
        db_table = 'detail_main_residential'
    id_residential_listing = models.OneToOneField(Residential_Listings, primary_key=True, db_column='id_residential_listing')
    amenities = models.TextField(null=True)
    available_from = models.DateTimeField(null=True)
    bathrooms = models.PositiveSmallIntegerField(null=True)
    bedrooms = models.PositiveSmallIntegerField(null=True)
    best_contact_time = models.DateTimeField(null=True)
    end_date = models.DateTimeField(null=True)
    ideal_tenant = models.TextField(null=True)
    listing_group = models.CharField(max_length=30, null=True)
    max_tenants = models.SmallIntegerField(null=True)
    parking = models.TextField(null=True)
    pets_okay = models.NullBooleanField()
    price_display = models.CharField(max_length=30, null=True)
    property_id = models.CharField(max_length=20, null=True)
    property_type = models.CharField(max_length=20, null=True)
    smokers_okay = models.NullBooleanField()
    start_date = models.DateTimeField(null=True)
    start_price = models.DecimalField(max_digits=11, decimal_places=2, null=True)
    title = models.CharField(max_length=100, null=True)
    rent_per_week = models.DecimalField(max_digits=11, decimal_places=2, null=True)
    whiteware = models.CharField(max_length=255, null=True)
    last_updated = models.DateTimeField()

class Agency_Brand(models.Model):
    class Meta:
        db_table = 'agency_brand'
    id_agency_brand = models.AutoField(primary_key=True, db_column='id_agency_brand')
    id_agency = models.ForeignKey(Agency, db_column='id_agency')
    id_agency_tm = models.IntegerField()
    logo1 = models.URLField(null=True)
    logo2 = models.URLField(null=True)
    brand_background_color = models.CharField(max_length=20, null=True)
    brand_stroke_color = models.CharField(max_length=20, null=True)
    brand_text_color = models.CharField(max_length=20, null=True)
    brand_large_banner = models.URLField(null=True)
    brand_disable_banner = models.NullBooleanField()
    brand_office_location = models.CharField(max_length=100)
    last_updated = models.DateTimeField()

class Agent_Rental_Detail(models.Model):
    class Meta:
        db_table = 'agent_rental_detail'
    id_rental_listing = models.ForeignKey(Rental_Listings, db_column='id_rental_listing')
    id_agent = models.ForeignKey(Agent, db_column='id_agent')
    last_updated = models.DateTimeField()

class Agent_Residential_Detail(models.Model):
    class Meta:
        db_table = 'agent_residential_detail'
    id_residential_listing = models.ForeignKey(Residential_Listings, db_column='id_residential_listing')
    id_agent = models.ForeignKey(Agent, db_column='id_agent')
    last_updated = models.DateTimeField()

class Region(models.Model):  # SCD 1
    class Meta:
        db_table = 'region'
    id_region = models.AutoField(primary_key=True)
    id_region_tm = models.IntegerField()
    name = models.CharField(max_length=40)
    last_updated = models.DateTimeField()

class District(models.Model):  # SCD 1
    class Meta:
        db_table = 'district'
    id_district = models.AutoField(primary_key=True)
    id_district_tm = models.IntegerField()
    name = models.CharField(max_length=40)
    last_updated = models.DateTimeField()

class Suburb(models.Model):  # SCD 1
    class Meta:
        db_table = 'suburb'
    id_suburb = models.AutoField(primary_key=True)
    id_suburb_tm = models.IntegerField()
    name = models.CharField(max_length=40)
    last_updated = models.DateTimeField()

class Location(models.Model):
    class Meta:
        db_table = 'location'
    id_location = models.AutoField(primary_key=True)
    id_region = models.ForeignKey(Region, db_column='id_region')
    id_district = models.ForeignKey(District, db_column='id_district')
    id_suburb = models.ForeignKey(Suburb, db_column='id_suburb')
    last_updated = models.DateTimeField()

class Rental_Listing_Location(models.Model):
    class Meta:
        db_table = 'rental_listing_location'
    id_rental_listing = models.OneToOneField(Rental_Listings, primary_key=True, db_column='id_rental_listing')
    id_location = models.ForeignKey(Location, db_column='id_location')
    address = models.CharField(max_length=255, null=True)
    latitude = models.CharField(max_length=20)
    longitude = models.CharField(max_length=20)
    easting = models.CharField(max_length=20)
    northing = models.CharField(max_length=20)
    last_updated = models.DateTimeField()

class Residential_Listing_Location(models.Model):
    class Meta:
        db_table = 'residential_listing_location'
    id_residential_listing = models.OneToOneField(Rental_Listings, primary_key=True, db_column='id_residential_listing')
    id_location = models.ForeignKey(Location, db_column='id_location')
    address = models.CharField(max_length=255, null=True)
    latitude = models.CharField(max_length=20)
    longitude = models.CharField(max_length=20)
    easting = models.CharField(max_length=20)
    northing = models.CharField(max_length=20)
    last_updated = models.DateTimeField()

class Adjacent_Suburbs(models.Model):
    class Meta:
        db_table = 'adjacent_suburbs'
    id_suburb = models.ForeignKey(Suburb, db_column='id_suburb')
    id_suburb_adjacent = models.IntegerField()
    last_updated = models.DateTimeField()

class Tm_Detail_Sub_Rental(models.Model):
    class Meta:
        db_table = 'tm_detail_sub_rental'
    id_rental_listing = models.OneToOneField(Rental_Listings, primary_key=True, db_column='id_rental_listing')
    agency_reference = models.CharField(max_length=30)
    category = models.CharField(max_length=30, null=True)
    category_path = models.CharField(max_length=100, null=True)
    has_embedded_video = models.NullBooleanField()
    has_gallery = models.NullBooleanField()
    is_bold = models.NullBooleanField()
    is_featured = models.NullBooleanField()
    is_highlighted = models.NullBooleanField()
    is_super_featured = models.NullBooleanField()
    listing_length = models.CharField(max_length=20, null=True)  # what is this?
    note_date = models.DateTimeField(null=True)
    reserve_state = models.SmallIntegerField(null=True)
    is_boosted = models.NullBooleanField()
    last_updated = models.DateTimeField()

class Tm_Detail_Sub_Residential(models.Model):
    class Meta:
        db_table = 'tm_detail_sub_residential'
    id_residential_listing = models.OneToOneField(Residential_Listings, primary_key=True, db_column='id_residential_listing')
    agency_reference = models.CharField(max_length=30)
    category = models.CharField(max_length=30, null=True)
    category_path = models.CharField(max_length=100, null=True)
    has_embedded_video = models.NullBooleanField()
    has_gallery = models.NullBooleanField()
    is_bold = models.NullBooleanField()
    is_featured = models.NullBooleanField()
    is_highlighted = models.NullBooleanField()
    is_super_featured = models.NullBooleanField()
    listing_length = models.CharField(max_length=20, null=True)  # what is this?
    note_date = models.DateTimeField(null=True)
    reserve_state = models.SmallIntegerField(null=True)
    is_boosted = models.NullBooleanField()
    last_updated = models.DateTimeField()

class Rental_Photo(models.Model):
    class Meta:
        db_table = 'rental_photo'
    id_rental_listing = models.ForeignKey(Rental_Listings, db_column='id_rental_listing')
    photo_key = models.CharField(max_length=20)
    last_updated = models.DateTimeField()

class Residential_Photo(models.Model):
    class Meta:
        db_table = 'residential_photo'
    id_residential_listing = models.ForeignKey(Residential_Listings, db_column='id_residential_listing')
    photo_key = models.CharField(max_length=20)
    last_updated = models.DateTimeField()

class Photo_Type_Helper(models.Model):
    class Meta:
        db_table = 'photo_type_helper'
    id_photo_type = models.SmallIntegerField(primary_key=True)
    url_name = models.CharField(max_length=30)
    description = models.CharField(max_length=50, null=True)
    last_updated = models.DateTimeField()
