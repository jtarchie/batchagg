# frozen_string_literal: true

require "active_record"
require "rspec-sqlimit"
ActiveRecord::Base.establish_connection(adapter: "sqlite3", database: ":memory:")

RSpec.describe "BatchAgg DSL" do
  include BatchAgg::DSL

  # Test models setup
  with_model :User do
    table do |t|
      t.integer :age
      t.string :name
    end

    model do
      has_many :posts
      has_one :profile
    end
  end

  with_model :Post do
    table do |t|
      t.string :title
      t.integer :views, default: 0
      t.belongs_to :user
    end
    model do
      belongs_to :user
    end
  end

  with_model :Category do
    table do |t|
      t.string :name
      t.integer :priority, default: 0
    end
    model do
      has_many :posts
    end
  end

  with_model :Profile do
    table do |t|
      t.string :bio
      t.boolean :verified, default: false
      t.integer :views, default: 0
      t.belongs_to :user
    end

    model do
      belongs_to :user
    end
  end

  # Test models for has_many :through
  with_model :Physician do
    table
    model do
      has_many :appointments, dependent: :destroy
      has_many :patients, through: :appointments
    end
  end

  with_model :Patient do
    table do |t|
      t.string :name
    end
    model do
      has_many :appointments, dependent: :destroy
      has_many :physicians, through: :appointments
    end
  end

  with_model :Appointment do
    table do |t|
      t.belongs_to :physician
      t.belongs_to :patient
      t.datetime :appointment_date
    end
    model do
      belongs_to :physician
      belongs_to :patient
    end
  end

  describe "basic aggregation functionality" do
    context "with single user" do
      let(:user) { User.create!(name: "Alice") }

      before do
        10.times { |i| user.posts.create!(title: "Post #{i + 1}") }
      end

      it "performs basic count aggregations with single query" do
        klass = aggregate(User) do
          count(:total_posts, &:posts)
          count(:posts_with_title) { |user_scope| user_scope.posts.where(title: "Post 1") }
        end

        expect { @agg = klass.only(user) }.to_not exceed_query_limit(1)

        expect(@agg[user.id].total_posts).to eq(10)
        expect(@agg[user.id].posts_with_title).to eq(1)
      end
    end

    context "with multiple users" do
      let!(:user1) { User.create!(name: "Alice") }
      let!(:user2) { User.create!(name: "Bob") }

      before do
        5.times { |i| user1.posts.create!(title: "Post #{i + 1}") }
        3.times { |i| user2.posts.create!(title: "Post #{i + 1}") }
      end

      it "handles multiple users with single query" do
        klass = aggregate(User) do
          count(:total_posts, &:posts)
          count(:posts_with_title) { |user_scope| user_scope.posts.where(title: "Post 1") }
        end

        expect { @results = klass.from(User.all) }.to_not exceed_query_limit(1)

        expect(@results[user1.id].total_posts).to eq(5)
        expect(@results[user1.id].posts_with_title).to eq(1)
        expect(@results[user2.id].total_posts).to eq(3)
        expect(@results[user2.id].posts_with_title).to eq(1)
      end
    end
  end

  describe "aggregate function types" do
    let(:user) { User.create!(name: "Alice") }

    before do
      user.posts.create!(title: "Post 1", views: 100)
      user.posts.create!(title: "Post 2", views: 200)
      user.posts.create!(title: "Post 3", views: 300)
    end

    context "column-based aggregations" do
      it "supports count, sum, avg, min, max on columns" do
        klass = aggregate(User) do
          count(:total_posts, &:posts)
          sum(:total_views, :views, &:posts)
          avg(:avg_views, :views, &:posts)
          min(:min_views, :views, &:posts)
          max(:max_views, :views, &:posts)
        end

        expect { @agg = klass.only(user) }.to_not exceed_query_limit(1)

        expect(@agg[user.id].total_posts).to eq(3)
        expect(@agg[user.id].total_views).to eq(600)
        expect(@agg[user.id].avg_views.to_f).to eq(200.0)
        expect(@agg[user.id].min_views).to eq(100)
        expect(@agg[user.id].max_views).to eq(300)
      end

      it "supports distinct counting" do
        # Setup data with duplicates
        Post.connection.add_column Post.table_name, :category_id, :integer
        Post.reset_column_information
        user.posts.first.update!(category_id: 1)
        user.posts.second.update!(category_id: 2)
        user.posts.third.update!(category_id: 1)

        klass = aggregate(User) do
          count_distinct(:unique_categories, :category_id, &:posts)
          count_distinct(:distinct_view_counts, :views, &:posts)
        end

        expect { @agg = klass.only(user) }.to_not exceed_query_limit(1)

        expect(@agg[user.id].unique_categories).to eq(2)
        expect(@agg[user.id].distinct_view_counts).to eq(3)
      end

      it "supports string aggregation" do
        klass = aggregate(User) do
          string_agg(:all_post_titles, :title, delimiter: "; ", &:posts)
        end

        expect { @agg = klass.only(user) }.to_not exceed_query_limit(1)

        titles = @agg[user.id].all_post_titles.split("; ").sort
        expect(titles).to match_array(["Post 1", "Post 2", "Post 3"])
      end
    end

    context "expression-based aggregations" do
      before do
        Post.connection.add_column Post.table_name, :likes, :integer, default: 0
        Post.reset_column_information
        user.posts.first.update!(likes: 10)
        user.posts.second.update!(likes: 20)
        user.posts.third.update!(likes: 15)
      end

      it "supports mathematical expressions in aggregations" do
        klass = aggregate(User) do
          sum_expression(:total_engagement, "views + likes", &:posts)
          sum_expression(:weighted_score, "views * 2 + likes * 5", &:posts)
          avg_expression(:avg_total_score, "views + likes", &:posts)
          min_expression(:min_total_score, "views + likes", &:posts)
          max_expression(:max_total_score, "views + likes", &:posts)
        end

        expect { @agg = klass.only(user) }.to_not exceed_query_limit(1)

        expect(@agg[user.id].total_engagement).to eq(645) # (100+10) + (200+20) + (300+15)
        expect(@agg[user.id].weighted_score).to eq(1425) # (100*2+10*5) + (200*2+20*5) + (300*2+15*5)
        expect(@agg[user.id].avg_total_score.to_f).to eq(215.0) # (110 + 220 + 315) / 3
        expect(@agg[user.id].min_total_score).to eq(110)
        expect(@agg[user.id].max_total_score).to eq(315)
      end

      it "supports conditional counting and distinct expressions" do
        Post.connection.add_column Post.table_name, :status, :string, default: "draft"
        Post.reset_column_information
        user.posts.first.update!(status: "published")
        user.posts.second.update!(status: "draft")
        user.posts.third.update!(status: "published")

        klass = aggregate(User) do
          count_expression(:published_posts, "CASE WHEN status = 'published' THEN 1 END", &:posts)
          count_expression(:high_view_posts, "CASE WHEN views > 150 THEN 1 END", &:posts)
          count_distinct_expression(:unique_status_view_combo, "status || '-' || CAST(views AS TEXT)", &:posts)
        end

        expect { @agg = klass.only(user) }.to_not exceed_query_limit(1)

        expect(@agg[user.id].published_posts).to eq(2)
        expect(@agg[user.id].high_view_posts).to eq(2) # Posts 2 and 3
        expect(@agg[user.id].unique_status_view_combo).to eq(3) # All combinations are unique
      end

      it "supports string expression aggregation" do
        Post.connection.add_column Post.table_name, :priority, :integer, default: 1
        Post.reset_column_information
        user.posts.first.update!(priority: 1)
        user.posts.second.update!(priority: 2)
        user.posts.third.update!(priority: 3)

        klass = aggregate(User) do
          string_agg_expression(:priority_titles, "priority || ': ' || title", delimiter: " | ", &:posts)
          string_agg_expression(:title_lengths, "title || ' (' || LENGTH(title) || ' chars)'", delimiter: "; ", &:posts)
        end

        expect { @agg = klass.only(user) }.to_not exceed_query_limit(1)

        priority_titles = @agg[user.id].priority_titles.split(" | ").sort
        expect(priority_titles).to match_array(["1: Post 1", "2: Post 2", "3: Post 3"])

        title_lengths = @agg[user.id].title_lengths.split("; ")
        expect(title_lengths.all? { |tl| tl.include?("chars)") }).to be true
      end
    end
  end

  describe "column projections and attribute access" do
    let(:user) { User.create!(name: "Alice", age: 30) }

    before do
      user.posts.create!(title: "First Post", views: 100)
    end

    it "supports direct column access and aliased columns" do
      klass = aggregate(User) do
        column(:age)
        column(:name)
        column(:aliased_age, &:age)
        column(:first_post_views) { |user_scope| user_scope.posts.limit(1).select(:views) }
      end

      expect { @agg = klass.only(user) }.to_not exceed_query_limit(1)

      expect(@agg[user.id].age).to eq(30)
      expect(@agg[user.id].name).to eq("Alice")
      expect(@agg[user.id].aliased_age).to eq(30)
      expect(@agg[user.id].first_post_views).to eq(100)
    end

    it "handles column name conflicts between models correctly" do
      User.connection.add_column User.table_name, :views, :integer, default: 0
      User.reset_column_information
      user.reload.update!(views: 50)

      klass = aggregate(User) do
        sum(:total_post_views, :views, &:posts) # Should use posts.views, not users.views
        column(:user_views, &:views) # Should use users.views
      end

      expect { @agg = klass.only(user) }.to_not exceed_query_limit(1)

      expect(@agg[user.id].total_post_views).to eq(100) # Post views
      expect(@agg[user.id].user_views).to eq(50) # User views
    end
  end

  describe "association support" do
    context "has_one associations" do
      let!(:user_with_profile) { User.create!(name: "Diana") }
      let!(:user_without_profile) { User.create!(name: "Eric") }

      before do
        user_with_profile.create_profile!(bio: "Architect", verified: true, views: 120)
      end

      it "handles has_one associations for users with and without associated records" do
        klass = aggregate(User) do
          column(:name)
          count(:has_profile, &:profile)
          column(:bio) { |user_scope| user_scope.profile.select(:bio) }
          column(:verified) { |user_scope| user_scope.profile.select(:verified) }
          sum(:profile_views, :views, &:profile)
        end

        expect { @results = klass.from(User.all) }.to_not exceed_query_limit(1)

        # User with profile
        expect(@results[user_with_profile.id].name).to eq("Diana")
        expect(@results[user_with_profile.id].has_profile).to eq(1)
        expect(@results[user_with_profile.id].bio).to eq("Architect")
        expect(@results[user_with_profile.id].verified).to eq(1)
        expect(@results[user_with_profile.id].profile_views).to eq(120)

        # User without profile
        expect(@results[user_without_profile.id].name).to eq("Eric")
        expect(@results[user_without_profile.id].has_profile).to eq(0)
        expect(@results[user_without_profile.id].bio).to be_nil
        expect(@results[user_without_profile.id].verified).to be_nil
        expect(@results[user_without_profile.id].profile_views).to eq(0)
      end
    end

    context "has_many :through associations" do
      let!(:physician1) { Physician.create! }
      let!(:physician2) { Physician.create! }
      let!(:patient1) { Patient.create!(name: "Pat1") }
      let!(:patient2) { Patient.create!(name: "Pat2") }
      let!(:patient3) { Patient.create!(name: "Pat3") }

      before do
        # Physician1 sees Pat1, Pat2
        Appointment.create!(physician: physician1, patient: patient1)
        Appointment.create!(physician: physician1, patient: patient2)

        # Physician2 sees Pat2, Pat3
        Appointment.create!(physician: physician2, patient: patient2)
        Appointment.create!(physician: physician2, patient: patient3)
      end

      it "handles has_many :through associations correctly" do
        klass = aggregate(Physician) do
          count(:total_patients, &:patients)
          string_agg(:patient_names, :name, delimiter: ", ", &:patients)
        end

        expect { @results = klass.from(Physician.all) }.to_not exceed_query_limit(1)

        expect(@results[physician1.id].total_patients).to eq(2)
        physician1_names = @results[physician1.id].patient_names.split(", ").sort
        expect(physician1_names).to match_array(%w[Pat1 Pat2])

        expect(@results[physician2.id].total_patients).to eq(2)
        physician2_names = @results[physician2.id].patient_names.split(", ").sort
        expect(physician2_names).to match_array(%w[Pat2 Pat3])
      end
    end
  end

  describe "complex queries and joins" do
    let(:user) { User.create!(name: "Alice", age: 30) }
    let!(:tech_category) { Category.create!(name: "Technology", priority: 1) }
    let!(:sports_category) { Category.create!(name: "Sports", priority: 2) }

    before do
      Post.connection.add_column Post.table_name, :category_id, :integer
      Post.connection.add_column Post.table_name, :published, :boolean, default: true
      Post.reset_column_information
      Post.class_eval { belongs_to :category, optional: true }

      user.posts.create!(title: "Tech Post 1", views: 100, category: tech_category, published: true)
      user.posts.create!(title: "Tech Post 2", views: 200, category: tech_category, published: false)
      user.posts.create!(title: "Sports Post 1", views: 150, category: sports_category, published: true)
      user.posts.create!(title: "Uncategorized Post", views: 50, published: true)
    end

    it "handles complex joins and filtering conditions" do
      klass = aggregate(User) do
        column(:age)
        count(:published_tech_posts) do |user_scope|
          user_scope.posts
                    .joins(:category)
                    .where(Category.table_name => { name: "Technology" }, published: true)
        end
        sum(:high_priority_views, :views) do |user_scope|
          user_scope.posts
                    .joins(:category)
                    .where(Category.table_name => { priority: [1, 2] })
        end
        sum_expression(:category_weighted_score,
                       "#{Post.table_name}.views * #{Category.table_name}.priority") do |user_scope|
          user_scope.posts.joins(:category)
        end
      end

      expect { @agg = klass.only(user) }.to_not exceed_query_limit(1)

      expect(@agg[user.id].age).to eq(30)
      expect(@agg[user.id].published_tech_posts).to eq(1) # Only Tech Post 1 is published
      expect(@agg[user.id].high_priority_views).to eq(450) # 100 + 200 + 150 (all categorized posts)
      expect(@agg[user.id].category_weighted_score).to eq(600) # (100*1) + (200*1) + (150*2)
    end
  end

  describe "edge cases and null handling" do
    it "handles aggregations returning NULL by defaulting to appropriate values" do
      user_no_posts = User.create!(name: "UserWithoutPosts")

      klass = aggregate(User) do
        sum(:sum_of_views, :views, &:posts)
        avg(:avg_of_views, :views, &:posts)
        min(:min_of_views, :views, &:posts)
        max(:max_of_views, :views, &:posts)
        sum_expression(:sum_expr_views, "views * 2", &:posts)
        avg_expression(:avg_expr_views, "views * 2", &:posts)
        min_expression(:min_expr_views, "views * 2", &:posts)
        max_expression(:max_expr_views, "views * 2", &:posts)
      end

      expect { @agg = klass.only(user_no_posts) }.to_not exceed_query_limit(1)

      result = @agg[user_no_posts.id]
      expect(result.sum_of_views).to eq(0)
      expect(result.avg_of_views.to_f).to eq(0.0)
      expect(result.min_of_views).to eq(0)
      expect(result.max_of_views).to eq(0)
      expect(result.sum_expr_views).to eq(0)
      expect(result.avg_expr_views.to_f).to eq(0.0)
      expect(result.min_expr_views).to eq(0)
      expect(result.max_expr_views).to eq(0)
    end

    it "handles mixed column and expression aggregations in single query" do
      user = User.create!(name: "Alice")
      Post.connection.add_column Post.table_name, :rating, :decimal, precision: 3, scale: 1, default: 0.0
      Post.reset_column_information

      user.posts.create!(title: "Post A", views: 100, rating: 4.5)
      user.posts.create!(title: "Post B", views: 200, rating: 3.8)
      user.posts.create!(title: "Post C", views: 150, rating: 4.2)

      klass = aggregate(User) do
        # Column-based
        count(:total_posts, &:posts)
        sum(:total_views, :views, &:posts)
        avg(:avg_rating, :rating, &:posts)
        # Expression-based
        count_expression(:high_rated_posts, "CASE WHEN rating >= 4.0 THEN 1 END", &:posts)
        sum_expression(:view_rating_product, "views * rating", &:posts)
        string_agg_expression(:title_rating, "title || ' (' || rating || '★)'", delimiter: " | ", &:posts)
      end

      expect { @agg = klass.only(user) }.to_not exceed_query_limit(1)

      expect(@agg[user.id].total_posts).to eq(3)
      expect(@agg[user.id].total_views).to eq(450)
      expect(@agg[user.id].avg_rating.to_f).to be_within(0.01).of(4.17)
      expect(@agg[user.id].high_rated_posts).to eq(2)
      expect(@agg[user.id].view_rating_product.to_f).to be_within(0.01).of(1840.0)
      expect(@agg[user.id].title_rating).to include("★)")
    end
  end
end
