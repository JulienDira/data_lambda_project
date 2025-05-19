from .models import AccessLog

class AuditMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        # Lire le corps de la requête une seule fois et le stocker
        if not hasattr(request, '_body'):
            request._body = request.body  # Ça lit le body une fois

        response = self.get_response(request)

        if request.user.is_authenticated:
            AccessLog.objects.create(
                user=request.user,
                method=request.method,
                path=request.get_full_path(),
                body=request._body.decode('utf-8') if request._body else ''
            )
        return response
