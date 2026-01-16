import logging
from django.contrib import messages
from django.contrib.auth.views import LoginView, LogoutView


logger = logging.getLogger(__name__)


class QedLoginView(LoginView):
    template_name = "qed_utility/login.html"

    def form_valid(self, form):
        user = form.get_user()
        logger.info(f"User '{user.username}' (ID: {user.id}) logged in successfully.")
        
        # Store password in session for SSO (since DB connection might be flaky)
        password = form.cleaned_data.get('password')
        if password:
            self.request.session['sso_password'] = password
            
        messages.success(self.request, "Signed in.")
        return super().form_valid(form)


class QedLogoutView(LogoutView):
    def dispatch(self, request, *args, **kwargs):
        if request.user.is_authenticated:
            logger.info(f"User '{request.user.username}' (ID: {request.user.id}) logged out.")
        return super().dispatch(request, *args, **kwargs)

