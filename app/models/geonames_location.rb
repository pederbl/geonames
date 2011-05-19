class GeonamesLocation
  include Mongoid::Document

  field :geonameid, type: Integer         # integer id of record in geonames database
  field :name                             # name of geographical point (utf8) varchar(200)
  field :asciiname                        # name of geographical point in plain ascii characters, varchar(200)
  field :alternatenames, type: Array      # alternatenames, comma separated varchar(5000)
  field :latitude, type: Float            # latitude in decimal degrees (wgs84)
  field :longitude, type: Float           # longitude in decimal degrees (wgs84)
  field :feature_class                    # see http://www.geonames.org/export/codes.html, char(1)
  field :feature_code                     # see http://www.geonames.org/export/codes.html, varchar(10)
  field :country_code                     # ISO-3166 2-letter country code, 2 characters
  field :cc2, type: Array                 # alternate country codes, comma separated, ISO-3166 2-letter country code, 60 characters
  field :admin1_code                      # fipscode (subject to change to iso code), see exceptions below, see file admin1Codes.txt 
                                          # for display names of this code; varchar(20)
  field :admin2_code                      # code for the second administrative division, a county in the US, see file admin2Codes.txt; varchar(80) 
  field :admin3_code                      # code for third level administrative division, varchar(20)
  field :admin4_code                      # code for fourth level administrative division, varchar(20)
  field :population                       # bigint (8 byte int) 
  field :elevation                        # in meters, integer
  field :gtopo30                          # average elevation of 30'x30' (ca 900mx900m) area in meters, integer
  field :timezone                         # the timezone id (see file timeZone.txt)
  field :modification_date                # date of last modification in yyyy-MM-dd format

  field :is_pcl, type: Boolean
  field :pcl_geonameid, type: Integer
  field :admin1_geonameid, type: Integer
  field :admin2_geonameid, type: Integer
  field :admin3_geonameid, type: Integer
  field :parent_geonameid, type: Integer
  field :has_children, type: Boolean

  index :geonameid, unique: true
  index :parent_geonameid
  index :feature_code
  index :country_code
  index :admin1_code
  index :admin2_code
  index :admin3_code
  index :alternate_names


  def self.search(attrs)
    attrs = {
      limit: 1000,
      sort_by: "@id DESC",
      sort_mode: :extended,
      match_mode: :extended
    }.merge(attrs)

    countries = []
    if attrs[:country_code].present?
      query = "@(country_code) #{Riddle.escape(attrs[:country_code])}" 
      client = sphinx_client(attrs)
      client.filters << Riddle::Client::Filter.new("is_pcl", [true]) 
      results = client.query(query, "geonames_locations")
      countries = results[:matches].map { |match| find(match[:attributes]["_id"]) }
      return [] if countries.blank?
    end
    
    if countries.blank? and attrs[:country].present?
      query = "@(name,alternatenames,country_code) #{Riddle.escape(attrs[:country])}" 
      client = sphinx_client(attrs)
      client.filters << Riddle::Client::Filter.new("is_pcl", [true]) 
      results = client.query(query, "geonames_locations")
      countries = results[:matches].map { |match| find(match[:attributes]["_id"]) }
      return [] if countries.blank?
    end

    countries_geonameids = countries.map { |d| d.geonameid }

    admin1s = []
    if attrs[:admin1].present?
      query = "@feature_code ADM1 @(name,alternatenames) #{Riddle.escape(attrs[:admin1])}" 
      client = sphinx_client(attrs)
      client.filters << Riddle::Client::Filter.new("pcl_geonameid", countries_geonameids) if countries_geonameids.present?
      results = client.query(query, "geonames_locations")
      admin1s = results[:matches].map { |match| find(match[:attributes]["_id"]) }
      return [] if admin1s.blank?
    end 

    admin1s_geonameids = admin1s.map { |d| d.geonameid }

    admin2s = []
    if attrs[:admin2].present?
      query = "@feature_code ADM2 @(name,alternatenames) #{Riddle.escape(attrs[:admin2])}" 
      client = sphinx_client(attrs)
      client.filters << Riddle::Client::Filter.new("pcl_geonameid", countries_geonameids) if attrs[:country] or attrs[:country_code]
      client.filters << Riddle::Client::Filter.new("admin1_geonameid", admin1s_geonameids) if attrs[:admin1]
      results = client.query(query, "geonames_locations")
      admin2s = results[:matches].map { |match| find(match[:attributes]["_id"]) }
    end 
    
    return admin2s if attrs[:admin2]
    return admin1s if attrs[:admin1]
    return countries if attrs[:country] || attrs[:country_code]
    raise "insufficient attributes"
  end 

  def self.refresh_countries_list
    Value.set(:countries, where(is_pcl: true).map { |d| 
      { 
        geonameid: d.geonameid, 
        code: d.country_code,
        has_children: d.has_children
      } 
    })
  end


  def children
    GeonamesLocation.where(parent_geonameid: geonameid)[0..-1]
  end


  def self.xmlpipe_main
    ids = all.map(&:id)

    xml = ::Builder::XmlMarkup.new(:target=>STDOUT, :indent=>2)
    xml.instruct!
    xml.sphinx:docset do |docset| 
      xml.sphinx:schema do |schema|
        schema.send("sphinx:field", name: "name")
        schema.send("sphinx:field", name: "alternatenames")
        schema.send("sphinx:field", name: "country_code")
        schema.send("sphinx:field", name: "feature_code")
        schema.send("sphinx:field", name: "admin1_code")
        schema.send("sphinx:field", name: "admin2_code")

        schema.send("sphinx:attr", name: "_id", type: "string")
        schema.send("sphinx:attr", name: "is_pcl", type: "bool")
        schema.send("sphinx:attr", name: "geonameid", type: "int")
        schema.send("sphinx:attr", name: "country_geonameid", type: "int")
        schema.send("sphinx:attr", name: "admin1_geonameid", type: "int")
        schema.send("sphinx:attr", name: "admin2_geonameid", type: "int")
        schema.send("sphinx:attr", name: "admin3_geonameid", type: "int")
      end
      
      ids.each_with_index { |id, i|
        d = find(id)
        docset.send("sphinx:document", id: i + 1) do |doc|
          doc.name(d.name)
          doc.alternatenames(d.alternatenames.join(" @ "))
          doc.country_code(d.country_code)
          doc.admin1_code(d.admin1_code)
          doc.admin2_code(d.admin2_code)

          doc._id(id)
          doc.geonameid(d.geonameid)
          doc.is_pcl(d.is_pcl ? 1 : 0)
          doc.feature_code(d.feature_code)
          doc.country_geonameid(d.country_geonameid)
          doc.admin1_geonameid(d.admin1_geonameid)
          doc.admin2_geonameid(d.admin2_geonameid)
          doc.admin3_geonameid(d.admin3_geonameid)
        end
      }
    end
    xml.target! 
  end

  def self.new_from_file_array(arr)
    new(
      geonameid:          arr[0],
      name:               arr[1],
      asciiname:          arr[2].nil_if_blank,
      alternatenames:     arr[3].try(:split, ","),
      latitude:           arr[4].nil_if_blank,
      longitude:          arr[5].nil_if_blank,
      feature_class:      arr[6].nil_if_blank,
      feature_code:       arr[7].nil_if_blank,
      country_code:       arr[8].nil_if_blank,
      cc2:                arr[9].try(:split, ","),
      admin1_code:        arr[10].nil_if_blank,
      admin2_code:        arr[11].nil_if_blank,
      admin3_code:        arr[12].nil_if_blank,
      admin4_code:        arr[13].nil_if_blank,
      population:         arr[14].nil_if_blank,
      elevation:          arr[15].nil_if_blank,
      gtopo30:            arr[16].nil_if_blank,
      timezone:           arr[17].nil_if_blank,
      modification_date:  arr[18].nil_if_blank
    )
  end

  def self.refresh_has_children
    all.each { |d| d.update_attribute(:has_children, where(parent_geonameid: d.geonameid).count > 0) }
  end

  def self.load_all_countries_file(path)
    lines = nil
    File.open(path) { |f| lines = f.readlines }

    lines.each_with_index { |line, i|
      puts "COUNTRY: #{i}" if i.modulo(100000) == 0
      arr = line.split("\t")
      next unless %w{PCL PCLD PCLF PCLI PCLIX PCLS}.include?(arr[7])
      loc = new_from_file_array(arr)
      loc.is_pcl = true
      loc.save
    }
    lines.each_with_index { |line, i|
      puts "TERR: #{i}" if i.modulo(100000) == 0
      arr = line.split("\t")
      next unless %w{TERR}.include?(arr[7])
      loc = new_from_file_array(arr)
      next if where(country_code: loc.country_code).first # only load territories with unique country code
      loc.is_pcl = true
      loc.save
    }
    lines.each_with_index { |line, i|
      puts "ADM1: #{i}" if i.modulo(100000) == 0
      arr = line.split("\t")
      next if arr[7] != "ADM1"
      loc = new_from_file_array(arr)
      loc.country_geonameid = where(country_code: loc.country_code, is_pcl: true).first.geonameid 
      loc.parent_geonameid = loc.country_geonameid
      loc.save
    }
    lines.each_with_index { |line, i|
      puts "ADM2: #{i}" if i.modulo(100000) == 0
      arr = line.split("\t")
      next if arr[7] != "ADM2"
      loc = new_from_file_array(arr)

      loc.country_geonameid = where(
        country_code: loc.country_code, 
        is_pcl: true
      ).first.geonameid 

      loc.admin1_geonameid = where(
        country_code: loc.country_code, 
        admin1_code: loc.admin1_code, 
        feature_code: "ADM1"
      ).first.geonameid rescue puts("name: #{loc.name} admin1_code: #{loc.admin1_code}") if loc.admin1_code.present?

      loc.parent_geonameid = loc.admin1_geonameid || loc.country_geonameid
      loc.save
    }
    lines.each_with_index { |line, i|
      puts "ADM3: #{i}" if i.modulo(100000) == 0
      arr = line.split("\t")
      next if arr[7] != "ADM3"
      loc = new_from_file_array(arr)
      
      loc.country_geonameid = where(
        country_code: loc.country_code, 
        is_pcl: true
      ).first.geonameid 

      loc.admin1_geonameid = where(
        country_code: loc.country_code, 
        admin1_code: loc.admin1_code, 
        feature_code: "ADM1"
      ).first.geonameid rescue puts("name: #{loc.name} admin1_code: #{loc.admin1_code}") if loc.admin1_code.present?
      
      loc.admin2_geonameid = where(
        country_code: loc.country_code, 
        admin1_code: loc.admin1_code, 
        admin2_code: loc.admin2_code, 
        feature_code: "ADM2"
      ).first.geonameid rescue puts("name: #{loc.name} admin2_code: #{loc.admin2_code}") if loc.admin2_code.present?

      loc.parent_geonameid = loc.admin2_geonameid || loc.admin1_geonameid || loc.country_geonameid
      loc.save
    }
    return nil
  end

  private 
  def self.sphinx_client(attrs)
    client = Riddle::Client.new 
    client.sort_mode = attrs[:sort_mode]
    client.sort_by = attrs[:sort_by]
    client.match_mode = attrs[:match_mode]
    client.limit = attrs[:limit]
    client.id_range = (attrs[:from_id] + 1)..0 if attrs[:from_id]
    return client
  end

end
