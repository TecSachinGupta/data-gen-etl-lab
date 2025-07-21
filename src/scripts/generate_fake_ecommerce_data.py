import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
from faker import Faker
import json
import os
import csv
import gzip
import shutil
from typing import Generator, Dict, Any, Iterator, Optional, Tuple
import logging
from collections import defaultdict
import sys
import gc
from pathlib import Path
from tqdm import tqdm


# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class OptimizedEcommerceDataGenerator:
    def __init__(self, seed=42, batch_size=10000, max_file_size_gb=1.0, compression=True):
        """Initialize the optimized streaming data generator"""
        random.seed(seed)
        np.random.seed(seed)
        Faker.seed(seed)
        self.fake = Faker()
        self.batch_size = batch_size
        self.max_file_size_bytes = int(max_file_size_gb * 1024 * 1024 * 1024)  # Convert GB to bytes
        self.compression = compression
        
        # Optimized cache with size limits
        self.max_cache_size = 100000  # Limit cache size to prevent memory issues
        self.category_cache = []
        self.product_cache = []
        self.customer_cache = []
        self.order_cache = []
        self.order_totals_cache = defaultdict(dict)
        
        # Pre-generated data for performance
        self._pregenerate_static_data()
        
        # File tracking
        self.current_file_sizes = {}
        self.file_counters = defaultdict(int)
        
    def _pregenerate_static_data(self):
        """Pre-generate static data for better performance"""
        self.category_names = [
            'Electronics', 'Clothing', 'Books', 'Home & Garden', 'Sports & Outdoors',
            'Health & Beauty', 'Toys & Games', 'Automotive', 'Food & Beverages',
            'Jewelry', 'Shoes', 'Baby & Kids', 'Pet Supplies', 'Office Supplies',
            'Tools & Hardware', 'Music & Movies', 'Art & Crafts', 'Travel',
            'Kitchen & Dining', 'Furniture', 'Pet Care', 'Garden & Outdoor',
            'Industrial & Scientific', 'Handmade', 'Grocery & Gourmet Food', 
            'Collectibles', 'Tickets & Experiences', 'Musical Instruments', 
            'Games & Virtual Goods', 'Stationery'
        ]
        
        self.categories = {
            "Electronics": {
                "subcategories": [
                    "Smartphones", "Laptops", "Tablets", "Cameras", "Televisions", "Headphones"
                ],
                "brands": [
                    "Apple", "Samsung", "Sony", "LG", "Dell", "HP", "Canon", "Bose"
                ]
            },
            "Clothing": {
                "subcategories": [
                    "Men's Clothing", "Women's Clothing", "Children's Clothing", "Sportswear", "Outerwear"
                ],
                "brands": [
                    "Nike", "Adidas", "Levi's", "Zara", "H&M", "Under Armour"
                ]
            },
            "Books": {
                "subcategories": [
                    "Fiction", "Non-fiction", "Children's Books", "Textbooks", "Comics", "Educational"
                ],
                "brands": [
                    "Penguin", "HarperCollins", "Simon & Schuster", "Random House", "Scholastic", "Oxford", "Marvel"
                ]
            },
            "Home & Garden": {
                "subcategories": [
                    "Furniture", "Bedding", "Decor", "Gardening Tools", "Lighting", "Tools"
                ],
                "brands": [
                    "IKEA", "Ashley Furniture", "Home Depot", "Lowe's", "Wayfair", "Bosch", "Gardena"
                ]
            },
            "Sports & Outdoors": {
                "subcategories": [
                    "Exercise Equipment", "Camping Gear", "Cycling", "Team Sports", "Fitness Apparel", "Fishing"
                ],
                "brands": [
                    "Nike", "Adidas", "Columbia", "The North Face", "Under Armour", "Shimano"
                ]
            },
            "Health & Beauty": {
                "subcategories": [
                    "Makeup", "Skincare", "Haircare", "Supplements", "Personal Care", "Fragrances"
                ],
                "brands": [
                    "L'Oréal", "Neutrogena", "Dove", "Maybelline", "Olay", "Cetaphil"
                ]
            },
            "Toys & Games": {
                "subcategories": [
                    "Action Figures", "Board Games", "Dolls", "Educational Toys", "Puzzles"
                ],
                "brands": [
                    "LEGO", "Mattel", "Hasbro", "Fisher-Price", "Ravensburger", "Playmobil"
                ]
            },
            "Automotive": {
                "subcategories": [
                    "Car Accessories", "Tires", "Motor Oils", "Car Electronics", "Tools"
                ],
                "brands": [
                    "Bosch", "Michelin", "Goodyear", "Castrol", "Pioneer", "Mobil"
                ]
            },
            "Food & Beverages": {
                "subcategories": [
                    "Snacks", "Beverages", "Gourmet", "Organic Food", "Baking", "Condiments"
                ],
                "brands": [
                    "Coca-Cola", "Pepsi", "Nestle", "Kellogg's", "General Mills", "Kraft"
                ]
            },
            "Jewelry": {
                "subcategories": [
                    "Necklaces", "Rings", "Bracelets", "Earrings", "Watches"
                ],
                "brands": [
                    "Tiffany & Co.", "Pandora", "Cartier", "Rolex", "Swarovski"
                ]
            },
            "Shoes": {
                "subcategories": [
                    "Men's Shoes", "Women's Shoes", "Children's Shoes", "Athletic Shoes", "Boots", "Sandals"
                ],
                "brands": [
                    "Nike", "Adidas", "Puma", "Skechers", "Vans", "Clarks", "Dr. Martens"
                ]
            },
            "Baby & Kids": {
                "subcategories": [
                    "Baby Clothing", "Toys", "Diapers", "Car Seats", "Feeding", "Safety"
                ],
                "brands": [
                    "Pampers", "Fisher-Price", "Huggies", "Chicco", "Graco", "Carter’s"
                ]
            },
            "Pet Supplies": {
                "subcategories": [
                    "Pet Food", "Pet Toys", "Pet Beds", "Leashes & Collars", "Grooming", "Training"
                ],
                "brands": [
                    "Purina", "Pedigree", "Kong", "Hill's", "Royal Canin", "PetSafe"
                ]
            },
            "Office Supplies": {
                "subcategories": [
                    "Pens & Pencils", "Notebooks", "Desk Organizers", "Printers", "Filing", "Office Furniture"
                ],
                "brands": [
                    "Staples", "HP", "Brother", "Canon", "3M", "Office Depot"
                ]
            },
            "Tools & Hardware": {
                "subcategories": [
                    "Power Tools", "Hand Tools", "Tool Storage", "Fasteners", "Safety Gear", "Plumbing"
                ],
                "brands": [
                    "DeWalt", "Bosch", "Makita", "Stanley", "3M"
                ]
            },
            "Music & Movies": {
                "subcategories": [
                    "CDs", "Vinyl", "DVDs", "Blu-ray", "Streaming", "Merchandise"
                ],
                "brands": [
                    "Sony Music", "Universal Music", "Warner Bros.", "Disney", "Paramount"
                ]
            },
            "Art & Crafts": {
                "subcategories": [
                    "Paints", "Drawing Supplies", "Craft Kits", "Canvas & Paper", "Beading", "Paper Crafts"
                ],
                "brands": [
                    "Crayola", "Faber-Castell", "Winsor & Newton", "Cricut", "Michaels"
                ]
            },
            "Travel": {
                "subcategories": [
                    "Luggage", "Travel Accessories", "Guides", "Travel Clothing", "Backpacks", "Apparel"
                ],
                "brands": [
                    "Samsonite", "American Tourister", "Travelpro", "Tumi", "Lonely Planet"
                ]
            },
            "Kitchen & Dining": {
                "subcategories": [
                    "Cookware", "Dinnerware", "Cutlery", "Appliances", "Bakeware", "Dining", "Utensils"
                ],
                "brands": [
                    "T-fal", "KitchenAid", "Cuisinart", "Philips", "Le Creuset", "OXO"
                ]
            },
            "Furniture": {
                "subcategories": [
                    "Sofas", "Beds", "Tables", "Chairs", "Cabinets", "Living Room", "Outdoor", "Office"
                ],
                "brands": [
                    "IKEA", "Ashley", "La-Z-Boy", "Wayfair", "West Elm"
                ]
            },
            "Pet Care": {
                "subcategories": [
                    "Veterinary Services", "Pet Grooming", "Pet Medications", "Food & Treats", "Supplements", "Healthcare"
                ],
                "brands": [
                    "Banfield Pet Hospital", "VetriScience", "Seresto", "Frontline", "Greenies"
                ]
            },
            "Garden & Outdoor": {
                "subcategories": [
                    "Plants", "Gardening Tools", "Outdoor Furniture", "BBQ Grills", "Grilling", "Landscape"
                ],
                "brands": [
                    "Scotts", "Weber", "Black+Decker", "Gardena", "Toro"
                ]
            },
            "Industrial & Scientific": {
                "subcategories": [
                    "Lab Equipment", "Measuring Instruments", "Safety Supplies", "Sensors", "Adhesives", "Professional Tools"
                ],
                "brands": [
                    "Fluke", "3M", "Thermo Fisher", "Honeywell", "Beckman Coulter", "GE"
                ]
            },
            "Handmade": {
                "subcategories": [
                    "Handmade Jewelry", "Handmade Home Decor", "Handmade Clothing", "Toys", "Art"
                ],
                "brands": [
                    "Local Artisans", "Etsy Shops", "Handmade by You"
                ]
            },
            "Grocery & Gourmet Food": {
                "subcategories": [
                    "Organic", "Snacks", "Baking Supplies", "Dairy", "Pantry", "Specialty"
                ],
                "brands": [
                    "Whole Foods", "Trader Joe's", "Nature’s Path", "La Costeña", "Ghirardelli", "Godiva"
                ]
            },
            "Collectibles": {
                "subcategories": [
                    "Coins", "Stamps", "Action Figures", "Comics", "Trading Cards", "Memorabilia", "Figurines"
                ],
                "brands": [
                    "Funko", "Topps", "Hasbro", "NBA Cards", "Mickey Mouse"
                ]
            },
            "Tickets & Experiences": {
                "subcategories": [
                    "Concert Tickets", "Sporting Event Tickets", "Theater Tickets", "Travel Packages", "Tours", "Workshops"
                ],
                "brands": [
                    "Ticketmaster", "StubHub", "Eventbrite", "Live Nation", "Viator"
                ]
            },
            "Musical Instruments": {
                "subcategories": [
                    "Guitars", "Pianos", "Drums", "Wind Instruments", "Keyboards", "Accessories"
                ],
                "brands": [
                    "Yamaha", "Fender", "Gibson", "Roland", "Korg"
                ]
            },
            "Games & Virtual Goods": {
                "subcategories": [
                    "Video Games", "Game Accessories", "In-game Currency", "DLC", "e-Books", "Software"
                ],
                "brands": [
                    "Sony", "Microsoft", "Nintendo", "Electronic Arts", "Steam", "Epic Games"
                ]
            },
            "Stationery": {
                "subcategories": [
                    "Notebooks", "Pens", "Art Supplies", "Organizers", "Greeting Cards", "Planners"
                ],
                "brands": [
                    "Moleskine", "Staedtler", "Pilot", "Paper Mate", "Rhodia", "Leuchtturm1917"
                ]
            }
        }

        
        self.price_ranges = {
            'Electronics': (10, 3000),
            'Clothing': (5, 500),
            'Books': (3, 100),
            'Home & Garden': (10, 1000),
            'Sports & Outdoors': (10, 800),
            'Health & Beauty': (5, 200),
            'Toys & Games': (5, 150),
            'Automotive': (20, 2000),
            'Food & Beverages': (1, 50)
        }
        
        self.status_weights = [0.70, 0.15, 0.08, 0.04, 0.02, 0.01]
        self.statuses = ['Completed', 'Shipped', 'Processing', 'Cancelled', 'Returned', 'Refunded']
        self.payment_methods = ['Credit Card', 'Debit Card', 'PayPal', 'Bank Transfer', 'Cash on Delivery', 'Digital Wallet']
        
        # Pre-generate some product names for better performance
        self.product_names = [self.fake.catch_phrase() for _ in range(10000)]
        
        logger.info("Static data pre-generation completed")
    
    def _get_file_path(self, base_path: str, file_number: int = 0) -> str:
        """Generate file path with optional file number for splitting"""
        path = Path(base_path)
        if file_number > 0:
            stem = path.stem
            suffix = path.suffix
            return str(path.parent / f"{stem}_part_{file_number:03d}{suffix}")
        return str(path)
    
    def _should_split_file(self, filepath: str) -> bool:
        """Check if file should be split based on size"""
        try:
            return os.path.getsize(filepath) >= self.max_file_size_bytes
        except FileNotFoundError:
            return False
    
    def _manage_cache_size(self, cache_list: list, max_size: int = None):
        """Manage cache size to prevent memory issues"""
        if max_size is None:
            max_size = self.max_cache_size
        
        if len(cache_list) > max_size:
            # Keep only recent entries
            cache_list[:] = cache_list[-max_size//2:]
            gc.collect()  # Force garbage collection
    
    def category_generator(self, num_categories: int) -> Generator[Dict[str, Any], None, None]:
        """Generate categories with better performance"""
        num_categories = 184 if num_categories is None else num_categories

        def _yield_category(category_dict):
            """Helper function to handle common steps for yielding a category"""
            self.category_cache.append(category_dict)
            self._manage_cache_size(self.category_cache)
            return category_dict
        
        if num_categories == 184:
            category_id = 1
            for parent_cat in tqdm(self.category_names, desc="Generating Categories..."):
                if parent_cat in self.categories.keys():
                    for subcat in self.categories[parent_cat]['subcategories']:
                        category = {
                            'category_id': category_id,
                            'category_name': subcat,
                            'parent_category': parent_cat,
                            'created_at': self.fake.date_time_between(start_date='-2y', end_date='now').isoformat()
                        }
                        
                        yield _yield_category(category)
                        category_id += 1
                else:
                    # Handle case where parent_cat doesn't have subcategories
                    category = {
                        'category_id': category_id,
                        'category_name': parent_cat,
                        'parent_category': None,
                        'created_at': self.fake.date_time_between(start_date='-2y', end_date='now').isoformat()
                    }
                    
                    yield _yield_category(category)
                    category_id += 1
        else:
            for i in tqdm(range(num_categories), desc="Generating Categories..."):
                parent_cat = random.choice(self.category_names)
                if parent_cat in self.categories.keys():
                    cat_name = random.choice(self.categories[parent_cat]['subcategories'])
                else:
                    cat_name = parent_cat
                    
                category = {
                    'category_id': i + 1,
                    'category_name': cat_name,
                    'parent_category': parent_cat,
                    'created_at': self.fake.date_time_between(start_date='-2y', end_date='now').isoformat()
                }
                
                self.category_cache.append(category)
                self._manage_cache_size(self.category_cache)
                yield category
    
    def product_generator(self, num_products: int) -> Generator[Dict[str, Any], None, None]:
        """Generate products with optimized performance"""
        if not self.category_cache:
            raise ValueError("Categories must be generated first")
        
        for i in tqdm(range(num_products), desc="Generating Products..."):
            category = random.choice(self.category_cache)
            price_range = self.price_ranges.get(category['parent_category'], (10, 1000))
            price = round(random.uniform(*price_range), 2)
            
            product = {
                'product_id': i + 1,
                'product_name': random.choice(self.product_names),
                'category_id': category['category_id'],
                'brand': random.choice(self.categories[category['parent_category']]['brands']),
                'price': price,
                'cost': round(price * random.uniform(0.3, 0.8), 2),
                'stock_quantity': random.randint(0, 1000),
                'weight_kg': round(random.uniform(0.1, 50.0), 2),
                'dimensions': f"{random.randint(5, 200)}x{random.randint(5, 200)}x{random.randint(2, 100)}",
                'description': self.fake.text(max_nb_chars=150),
                'is_active': random.choices([True, False], weights=[0.85, 0.15])[0],
                'created_at': self.fake.date_time_between(start_date='-2y', end_date='now').isoformat()
            }
            
            self.product_cache.append(product)
            self._manage_cache_size(self.product_cache)
            yield product
    
    def customer_generator(self, num_customers: int) -> Generator[Dict[str, Any], None, None]:
        """Generate customers with optimized performance"""
        countries = ['USA', 'Canada', 'UK', 'Germany', 'France', 'Japan', 'Australia', 'India', 'Brazil', 'Mexico']
        
        for i in tqdm(range(num_customers), desc="Generating Customers..."):
            registration_date = self.fake.date_time_between(start_date='-3y', end_date='now')
            
            customer = {
                'customer_id': i + 1,
                'email': self.fake.email(),
                'first_name': self.fake.first_name(),
                'last_name': self.fake.last_name(),
                'phone': self.fake.phone_number(),
                'date_of_birth': self.fake.date_of_birth(minimum_age=16, maximum_age=85).isoformat(),
                'gender': random.choices(['M', 'F', 'Other'], weights=[0.48, 0.48, 0.04])[0],
                'country': random.choice(countries),
                'city': self.fake.city(),
                'postal_code': self.fake.postcode(),
                'address': self.fake.address().replace('\n', ' '),
                'registration_date': registration_date.isoformat(),
                'last_login': self.fake.date_time_between(start_date=registration_date, end_date='now').isoformat(),
                'is_active': random.choices([True, False], weights=[0.9, 0.1])[0],
                'customer_segment': random.choices(['Premium', 'Regular', 'Budget'], weights=[0.2, 0.6, 0.2])[0],
                'marketing_consent': random.choices([True, False], weights=[0.7, 0.3])[0]
            }
            
            self.customer_cache.append(customer)
            self._manage_cache_size(self.customer_cache)
            yield customer
    
    def order_generator(self, num_orders: int) -> Generator[Dict[str, Any], None, None]:
        """Generate orders with optimized performance"""
        if not self.customer_cache:
            raise ValueError("Customers must be generated first")
        
        for i in tqdm(range(num_orders), desc="Generating Orders..."):
            customer = random.choice(self.customer_cache)
            order_date = self.fake.date_time_between(
                start_date=datetime.fromisoformat(customer['registration_date']), 
                end_date='now'
            )
            
            status = random.choices(self.statuses, weights=self.status_weights)[0]
            
            order = {
                'order_id': i + 1,
                'customer_id': customer['customer_id'],
                'order_date': order_date.isoformat(),
                'status': status,
                'payment_method': random.choice(self.payment_methods),
                'shipping_address': self.fake.address().replace('\n', ' '),
                'billing_address': self.fake.address().replace('\n', ' '),
                'discount_amount': round(random.uniform(0, 100), 2) if random.random() < 0.25 else 0,
                'tax_amount': 0,
                'shipping_cost': round(random.uniform(0, 50), 2),
                'total_amount': 0,
                'currency': random.choices(['USD', 'EUR', 'GBP', 'CAD'], weights=[0.6, 0.2, 0.1, 0.1])[0],
                'created_at': order_date.isoformat(),
                'updated_at': (order_date + timedelta(days=random.randint(0, 7))).isoformat()
            }
            
            self.order_cache.append(order)
            self._manage_cache_size(self.order_cache)
            yield order
    
    def order_item_generator(self, avg_items_per_order: float = 2.8) -> Generator[Dict[str, Any], None, None]:
        """Generate order items with optimized performance"""
        if not self.order_cache or not self.product_cache:
            raise ValueError("Orders and products must be generated first")
        
        item_id = 1
        
        for order in self.order_cache:
            num_items = max(1, int(np.random.poisson(avg_items_per_order)))
            
            # Use random sampling for better performance
            available_products = min(num_items, len(self.product_cache))
            selected_products = random.sample(self.product_cache, available_products)
            
            order_subtotal = 0
            order_discount = 0
            
            for product in selected_products:
                quantity = random.randint(1, 10)
                unit_price = product['price']
                line_total = quantity * unit_price
                discount = round(line_total * random.uniform(0, 0.15), 2) if random.random() < 0.3 else 0
                
                order_item = {
                    'order_item_id': item_id,
                    'order_id': order['order_id'],
                    'product_id': product['product_id'],
                    'quantity': quantity,
                    'unit_price': unit_price,
                    'line_total': line_total,
                    'discount_amount': discount
                }
                
                order_subtotal += line_total
                order_discount += discount
                
                item_id += 1
                yield order_item
            
            # Cache order totals
            self.order_totals_cache[order['order_id']] = {
                'subtotal': order_subtotal,
                'item_discount': order_discount
            }
    
    def inventory_log_generator(self, num_logs: int) -> Generator[Dict[str, Any], None, None]:
        """Generate inventory logs with optimized performance"""
        if not self.product_cache:
            raise ValueError("Products must be generated first")
        
        movement_types = ['IN', 'OUT', 'ADJUSTMENT']
        reasons = ['Purchase', 'Sale', 'Return', 'Damage', 'Adjustment', 'Theft', 'Transfer']
        
        for i in tqdm(range(num_logs), desc="Generating Inventory Log..."):
            product = random.choice(self.product_cache)
            movement_type = random.choice(movement_types)
            quantity = random.randint(1, 500)
            
            if movement_type == 'OUT':
                quantity = -quantity
            elif movement_type == 'ADJUSTMENT':
                quantity = random.randint(-100, 100)
            
            yield {
                'log_id': i + 1,
                'product_id': product['product_id'],
                'movement_type': movement_type,
                'quantity_change': quantity,
                'reason': random.choice(reasons),
                'timestamp': self.fake.date_time_between(start_date='-2y', end_date='now').isoformat(),
                'reference_id': random.randint(10000, 99999),
                'notes': self.fake.sentence() if random.random() < 0.3 else ''
            }
    
    def review_generator(self, num_reviews: int) -> Generator[Dict[str, Any], None, None]:
        """Generate reviews with optimized performance"""
        if not self.customer_cache or not self.product_cache:
            raise ValueError("Customers and products must be generated first")
        
        for i in tqdm(range(num_reviews), desc="Generating Reviews..."):
            customer = random.choice(self.customer_cache)
            product = random.choice(self.product_cache)
            rating = np.random.choice([1, 2, 3, 4, 5], p=[0.03, 0.07, 0.15, 0.40, 0.35])
            
            yield {
                'review_id': i + 1,
                'customer_id': customer['customer_id'],
                'product_id': product['product_id'],
                'rating': rating,
                'title': self.fake.sentence(nb_words=random.randint(3, 8)),
                'comment': self.fake.text(max_nb_chars=500),
                'is_verified_purchase': random.choices([True, False], weights=[0.8, 0.2])[0],
                'helpful_votes': random.randint(0, 100),
                'created_at': self.fake.date_time_between(start_date='-2y', end_date='now').isoformat()
            }
    
    def stream_to_csv_with_splitting(self, generator: Generator, base_filename: str, 
                                   fieldnames: list = None) -> list:
        """Stream data to CSV with automatic file splitting"""
        logger.info(f"Starting to stream data to {base_filename} with file splitting")
        
        output_files = []
        current_file_num = 0
        current_filename = self._get_file_path(base_filename, current_file_num)
        
        def get_file_handle():
            nonlocal current_file_num, current_filename
            if self.compression:
                return gzip.open(current_filename + '.gz', 'wt', newline='', encoding='utf-8')
            else:
                return open(current_filename, 'w', newline='', encoding='utf-8')
        
        csvfile = get_file_handle()
        writer = None
        total_records = 0
        current_file_records = 0
        
        try:
            while True:
                batch = []
                
                # Collect batch
                for _ in range(self.batch_size):
                    try:
                        record = next(generator)
                        batch.append(record)
                    except StopIteration:
                        break
                
                if not batch:
                    break
                
                # Initialize writer with first record's keys
                if writer is None:
                    if fieldnames is None:
                        fieldnames = list(batch[0].keys())
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()
                
                # Write batch
                writer.writerows(batch)
                total_records += len(batch)
                current_file_records += len(batch)
                
                # Check if we need to split the file
                csvfile.flush()
                actual_filename = current_filename + '.gz' if self.compression else current_filename
                
                if self._should_split_file(actual_filename):
                    csvfile.close()
                    output_files.append(actual_filename)
                    
                    # Start new file
                    current_file_num += 1
                    current_filename = self._get_file_path(base_filename, current_file_num)
                    csvfile = get_file_handle()
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()
                    current_file_records = 0
                    
                    logger.info(f"  Split to new file: {current_filename}")
                
                if total_records % (self.batch_size * 100) == 0:
                    logger.info(f"  Streamed {total_records} records")
        
        finally:
            csvfile.close()
            actual_filename = current_filename + '.gz' if self.compression else current_filename
            output_files.append(actual_filename)
        
        logger.info(f"Completed streaming {total_records} records to {len(output_files)} files")
        return output_files
    
    def update_order_totals_optimized(self, orders_files: list) -> None:
        """Update order totals with optimized processing"""
        logger.info("Updating order totals across multiple files...")
        
        for orders_file in orders_files:
            logger.info(f"  Processing {orders_file}")
            
            # Handle compressed files
            is_compressed = orders_file.endswith('.gz')
            temp_file = f"{orders_file}.temp"
            
            if is_compressed:
                infile = gzip.open(orders_file, 'rt', encoding='utf-8')
                outfile = gzip.open(temp_file, 'wt', newline='', encoding='utf-8')
            else:
                infile = open(orders_file, 'r', encoding='utf-8')
                outfile = open(temp_file, 'w', newline='', encoding='utf-8')
            
            try:
                reader = csv.DictReader(infile)
                fieldnames = list(reader.fieldnames)
                
                if 'subtotal' not in fieldnames:
                    fieldnames.append('subtotal')
                
                writer = csv.DictWriter(outfile, fieldnames=fieldnames)
                writer.writeheader()
                
                for row in reader:
                    order_id = int(row['order_id'])
                    
                    if order_id in self.order_totals_cache:
                        totals = self.order_totals_cache[order_id]
                        subtotal = totals['subtotal']
                        item_discount = totals['item_discount']
                        
                        row['subtotal'] = subtotal
                        row['tax_amount'] = round(subtotal * 0.085, 2)  # 8.5% tax
                        row['total_amount'] = round(
                            subtotal - 
                            item_discount - 
                            float(row['discount_amount']) + 
                            float(row['tax_amount']) + 
                            float(row['shipping_cost']), 2
                        )
                    else:
                        row['subtotal'] = 0
                        row['tax_amount'] = 0
                        row['total_amount'] = float(row['shipping_cost'])
                    
                    writer.writerow(row)
                
            finally:
                infile.close()
                outfile.close()
            
            # Replace original file
            os.replace(temp_file, orders_file)
        
        logger.info("Order totals updated successfully")
    
    def generate_all_data_optimized(self, 
                                   output_dir: str = 'ecommerce_data_large',
                                   num_categories: int = 1000,
                                   num_products: int = 10000000,
                                   num_customers: int = 2000000,
                                   num_orders: int = 50000000,
                                   num_reviews: int = 5000000,
                                   num_inventory_logs: int = 10000000) -> Dict[str, list]:
        """Generate all data with optimizations for large datasets"""
        os.makedirs(output_dir, exist_ok=True)
        
        output_files = {}
        
        # Generate categories
        logger.info("Generating categories...")
        categories_gen = self.category_generator(num_categories)
        output_files['categories'] = self.stream_to_csv_with_splitting(
            categories_gen, f"{output_dir}/categories.csv"
        )
        
        # Generate products
        logger.info("Generating products...")
        products_gen = self.product_generator(num_products)
        output_files['products'] = self.stream_to_csv_with_splitting(
            products_gen, f"{output_dir}/products.csv"
        )
        
        # Generate customers
        logger.info("Generating customers...")
        customers_gen = self.customer_generator(num_customers)
        output_files['customers'] = self.stream_to_csv_with_splitting(
            customers_gen, f"{output_dir}/customers.csv"
        )
        
        # Generate orders
        logger.info("Generating orders...")
        orders_gen = self.order_generator(num_orders)
        output_files['orders'] = self.stream_to_csv_with_splitting(
            orders_gen, f"{output_dir}/orders.csv"
        )
        
        # Generate order items
        logger.info("Generating order items...")
        order_items_gen = self.order_item_generator()
        output_files['order_items'] = self.stream_to_csv_with_splitting(
            order_items_gen, f"{output_dir}/order_items.csv"
        )
        
        # Update order totals
        self.update_order_totals_optimized(output_files['orders'])
        
        # Generate inventory logs
        logger.info("Generating inventory logs...")
        inventory_gen = self.inventory_log_generator(num_inventory_logs)
        output_files['inventory_logs'] = self.stream_to_csv_with_splitting(
            inventory_gen, f"{output_dir}/inventory_logs.csv"
        )
        
        # Generate reviews
        logger.info("Generating reviews...")
        reviews_gen = self.review_generator(num_reviews)
        output_files['reviews'] = self.stream_to_csv_with_splitting(
            reviews_gen, f"{output_dir}/reviews.csv"
        )
        
        logger.info("Large dataset generation completed successfully!")
        return output_files
    
    def get_generation_stats(self) -> Dict[str, Any]:
        """Get comprehensive generation statistics"""
        return {
            'memory_usage': self.get_memory_usage(),
            'cache_limits': {
                'max_cache_size': self.max_cache_size,
                'batch_size': self.batch_size
            },
            'file_settings': {
                'max_file_size_gb': self.max_file_size_bytes / (1024**3),
                'compression_enabled': self.compression
            }
        }
    
    def get_memory_usage(self) -> Dict[str, int]:
        """Get current memory usage of cached data"""
        return {
            'categories_cached': len(self.category_cache),
            'products_cached': len(self.product_cache),
            'customers_cached': len(self.customer_cache),
            'orders_cached': len(self.order_cache),
            'order_totals_cached': len(self.order_totals_cache)
        }
    
    def clear_cache(self) -> None:
        """Clear all cached data to free memory"""
        self.category_cache.clear()
        self.product_cache.clear()
        self.customer_cache.clear()
        self.order_cache.clear()
        self.order_totals_cache.clear()
        gc.collect()
        logger.info("Cache cleared and garbage collected")
# Enhanced monitoring with file size tracking
class OptimizedDataGenerationMonitor:
    def __init__(self, generator: OptimizedEcommerceDataGenerator):
        self.generator = generator
        
    def format_file_size(self, size_bytes: int) -> str:
        """Format file size in human readable format"""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size_bytes < 1024:
                return f"{size_bytes:.2f} {unit}"
            size_bytes /= 1024
        return f"{size_bytes:.2f} TB"
    
    def monitor_generation(self, **kwargs) -> Dict[str, list]:
        """Monitor the optimized data generation process"""
        start_time = datetime.now()
        
        try:
            # Generate data
            output_files = self.generator.generate_all_data_optimized(**kwargs)
            
            # Calculate file sizes
            file_stats = {}
            total_size = 0
            
            for table_name, files in output_files.items():
                table_size = 0
                for file_path in files:
                    try:
                        file_size = os.path.getsize(file_path)
                        table_size += file_size
                        total_size += file_size
                    except FileNotFoundError:
                        continue
                
                file_stats[table_name] = {
                    'file_count': len(files),
                    'total_size': self.format_file_size(table_size),
                    'files': files
                }
            
            # Report completion
            end_time = datetime.now()
            duration = end_time - start_time
            
            print(f"\n{'='*80}")
            print("LARGE DATASET GENERATION COMPLETED")
            print(f"{'='*80}")
            print(f"Duration: {duration}")
            print(f"Total Size: {self.format_file_size(total_size)}")
            print(f"Output Directory: {kwargs.get('output_dir', 'ecommerce_data_large')}")
            print(f"Compression: {'Enabled' if self.generator.compression else 'Disabled'}")
            print(f"Max File Size: {self.generator.max_file_size_bytes / (1024**3):.1f} GB")
            
            print(f"\nFile Statistics:")
            print(f"{'-'*50}")
            for table_name, stats in file_stats.items():
                print(f"{table_name:20} | {stats['file_count']:3} files | {stats['total_size']:>10}")
            
            # Memory usage
            memory_usage = self.generator.get_memory_usage()
            print(f"\nFinal Memory Usage: {memory_usage}")
            
            return output_files
            
        except Exception as e:
            logger.error(f"Error during data generation: {e}")
            raise

# Usage example for large datasets
if __name__ == "__main__":
    # Initialize optimized generator
    generator = OptimizedEcommerceDataGenerator(
        seed=42, 
        batch_size=50000,  # Larger batch size for better performance
        max_file_size_gb=1.0,  # 1GB per file
        compression=False  # Enable compression
    )
    
    monitor = OptimizedDataGenerationMonitor(generator)
    
    # Generate massive dataset
    # output_files = monitor.monitor_generation(
        # output_dir='ecommerce_data',
        # num_categories=184,
        # num_products=25000000,      # 25 million products
        # num_customers=10000000,     # 10 million customers
        # num_orders=100000000,       # 100 million orders
        # num_reviews=20000000,       # 20 million reviews
        # num_inventory_logs=50000000 # 50 million inventory logs
    # )
    
    output_files = monitor.monitor_generation(
        output_dir='ecommerce_data',
        num_categories=184,
        num_products=2500000,      # 2.5 million products
        num_customers=1000000,     # 1 million customers
        num_orders=10000000,       # 10 million orders
        num_reviews=2000000,       # 2 million reviews
        num_inventory_logs=5000000 # 5 million inventory logs
    )
    
    # Print file information
    print(f"\n{'='*80}")
    print("GENERATED FILES SUMMARY")
    print(f"{'='*80}")
    
    for table_name, files in output_files.items():
        print(f"\n{table_name.upper()}:")
        for i, file_path in enumerate(files):
            try:
                size = os.path.getsize(file_path)
                print(f"  {i+1:3d}. {file_path} ({monitor.format_file_size(size)})")
            except FileNotFoundError:
                print(f"  {i+1:3d}. {file_path}")