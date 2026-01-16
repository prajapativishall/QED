import mysql.connector
from django.contrib.auth.backends import BaseBackend
from django.contrib.auth.models import User, Group
from qed_utility.views.dashboard import DB_CONFIG

class FlowableBackend(BaseBackend):
    def authenticate(self, request, username=None, password=None, **kwargs):
        if not username or not password:
            return None
        
        try:
            # Check credentials against Flowable DB
            with mysql.connector.connect(**DB_CONFIG) as conn:
                cursor = conn.cursor()
                # Check credentials against Flowable DB (Case-insensitive username)
                query = "SELECT ID_, FIRST_, LAST_, EMAIL_ FROM ACT_ID_USER WHERE LOWER(ID_) = LOWER(%s) AND PWD_ = %s"
                cursor.execute(query, (username, password))
                row = cursor.fetchone()
                
                if row:
                    user_id, first_name, last_name, email = row
                    
                    # Create or update Django user
                    try:
                        user = User.objects.get(username=user_id)
                        # Update details if changed
                        if user.first_name != first_name or user.last_name != last_name or user.email != email:
                            user.first_name = first_name or ""
                            user.last_name = last_name or ""
                            user.email = email or ""
                            user.save()
                    except User.DoesNotExist:
                        # Create new user
                        # We set an unusable password because auth happens via Flowable
                        user = User.objects.create_user(
                            username=user_id,
                            email=email or "",
                            password=None, # Sets unusable password
                            first_name=first_name or "",
                            last_name=last_name or ""
                        )
                    
                    # Sync Groups
                    self._sync_groups(cursor, user, user_id)
                    
                    return user
                    
        except Exception as e:
            # Log error properly in production
            print(f"Flowable authentication error: {e}")
            return None
            
        return None

    def _sync_groups(self, cursor, user, user_id):
        try:
            # Fetch groups from Flowable
            query = "SELECT GROUP_ID_ FROM ACT_ID_MEMBERSHIP WHERE USER_ID_ = %s"
            cursor.execute(query, (user_id,))
            flowable_groups = [row[0] for row in cursor.fetchall()]
            
            # Sync with Django groups
            current_groups = set(user.groups.values_list('name', flat=True))
            target_groups = set(flowable_groups)
            
            # Add to new groups
            for group_name in target_groups - current_groups:
                group, created = Group.objects.get_or_create(name=group_name)
                user.groups.add(group)
                
            # Remove from old groups (optional, but good for consistency)
            # We only remove groups that look like Flowable groups (to avoid removing local admin roles if mixed)
            # For now, let's just add. Removing might be dangerous if they manually added roles in Django.
            # But "Sync" implies matching. 
            # Let's assume Flowable is the source of truth for these users.
            # To be safe, I'll just ADD for now.
            
        except Exception as e:
            print(f"Error syncing groups: {e}")

    def get_user(self, user_id):
        try:
            return User.objects.get(pk=user_id)
        except User.DoesNotExist:
            return None
