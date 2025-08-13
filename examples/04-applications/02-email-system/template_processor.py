#!/usr/bin/env python3
"""
Email Template Processor - Template Management and Rendering

This example demonstrates advanced template processing features:
- Dynamic template rendering with context data
- Multi-language template support
- Template validation and sanitization
- Personalization and customization
- Template caching and optimization

Before running:
1. Start NATS: `cd docker && docker-compose up -d`
2. Set secure serializer: `export NAQ_JOB_SERIALIZER=json`
3. Start workers: `naq worker default email_template --log-level INFO`
4. Run this script: `python template_processor.py`
"""

import os
import re
import time
import uuid
import random
from datetime import datetime, timedelta
from html import escape
from typing import Dict, List, Any, Optional

from naq import SyncClient, setup_logging

# Configure secure JSON serialization
os.environ.setdefault('NAQ_JOB_SERIALIZER', 'json')

# Setup logging
setup_logging(level="INFO")


# ===== TEMPLATE PROCESSING FUNCTIONS =====

def render_email_template(template_name: str, context: Dict[str, Any], 
                         language: str = 'en', user_preferences: Optional[Dict[str, Any]] = None) -> str:
    """
    Render email template with comprehensive personalization.
    
    Args:
        template_name: Name of the template to render
        context: Template context data
        language: Language code for localization
        user_preferences: User-specific preferences
        
    Returns:
        Rendered template result
    """
    template_id = str(uuid.uuid4())[:8]
    
    print(f"🎨 Rendering template: {template_name}")
    print(f"   Template ID: {template_id}")
    print(f"   Language: {language}")
    print(f"   Context keys: {list(context.keys())}")
    
    # Simulate template loading
    time.sleep(0.3)
    
    # Add user preferences to context
    if user_preferences:
        context.update({
            'user_timezone': user_preferences.get('timezone', 'UTC'),
            'user_language': user_preferences.get('language', language),
            'user_name': user_preferences.get('name', 'Friend'),
            'frequency_preference': user_preferences.get('frequency', 'weekly')
        })
    
    # Add standard template variables
    context.update({
        'current_date': datetime.utcnow().strftime('%Y-%m-%d'),
        'current_year': datetime.utcnow().year,
        'unsubscribe_url': f"https://example.com/unsubscribe?token={template_id}",
        'tracking_pixel': f"https://example.com/track/{template_id}.gif",
        'company_name': 'NAQ Email System',
        'support_email': 'support@naq-system.com'
    })
    
    # Template-specific rendering logic
    if template_name == 'welcome':
        subject = get_localized_subject('welcome', language)
        html_content = render_welcome_template(context, language)
    elif template_name == 'newsletter':
        subject = get_localized_subject('newsletter', language)
        html_content = render_newsletter_template(context, language)
    elif template_name == 'order_confirmation':
        subject = get_localized_subject('order_confirmation', language)
        html_content = render_order_template(context, language)
    elif template_name == 'password_reset':
        subject = get_localized_subject('password_reset', language)
        html_content = render_password_reset_template(context, language)
    else:
        subject = f"Email from {context.get('company_name', 'NAQ System')}"
        html_content = render_generic_template(template_name, context, language)
    
    # Generate text version
    text_content = html_to_text(html_content)
    
    # Validate and sanitize content
    html_content = sanitize_html_content(html_content)
    text_content = sanitize_text_content(text_content)
    
    result = {
        'template_id': template_id,
        'subject': subject,
        'html_content': html_content,
        'text_content': text_content,
        'language': language,
        'render_time': datetime.utcnow().isoformat()
    }
    
    print(f"✅ Template rendered: {len(html_content)} chars HTML, {len(text_content)} chars text")
    
    return f"Template {template_name} rendered successfully ({template_id})"


def validate_template_content(template_content: str, template_type: str = 'html') -> str:
    """
    Validate template content for security and compliance.
    
    Args:
        template_content: Raw template content
        template_type: Type of template (html, text, subject)
        
    Returns:
        Validation result
    """
    print(f"🔍 Validating template content: {template_type}")
    print(f"   Content length: {len(template_content)} characters")
    
    validation_errors = []
    warnings = []
    
    # Check for dangerous patterns
    dangerous_patterns = [
        (r'<script.*?</script>', 'Script tags detected'),
        (r'javascript:', 'JavaScript URLs detected'),
        (r'on\w+\s*=', 'Event handlers detected'),
        (r'eval\s*\(', 'eval() calls detected'),
        (r'exec\s*\(', 'exec() calls detected'),
        (r'<iframe', 'Iframe tags detected')
    ]
    
    for pattern, message in dangerous_patterns:
        if re.search(pattern, template_content, re.IGNORECASE):
            validation_errors.append(message)
    
    # Check for compliance issues
    if template_type == 'html':
        # Check for required elements
        if 'unsubscribe' not in template_content.lower():
            warnings.append('Missing unsubscribe link')
        
        if not re.search(r'{{.*company.*}}', template_content, re.IGNORECASE):
            warnings.append('Missing company information placeholder')
        
        # Check for accessibility
        if '<img' in template_content and 'alt=' not in template_content:
            warnings.append('Images missing alt text')
    
    # Length validation
    if template_type == 'subject' and len(template_content) > 100:
        warnings.append('Subject line may be too long for mobile devices')
    
    if template_type == 'html' and len(template_content) > 100000:
        warnings.append('Template content is very large')
    
    # Simulate validation processing
    time.sleep(0.5)
    
    # Report results
    if validation_errors:
        print(f"   ❌ Validation failed: {len(validation_errors)} errors")
        for error in validation_errors:
            print(f"     • {error}")
        result = f"Template validation failed: {len(validation_errors)} errors found"
    else:
        print(f"   ✅ Validation passed")
        if warnings:
            print(f"   ⚠️  {len(warnings)} warnings:")
            for warning in warnings:
                print(f"     • {warning}")
        result = f"Template validation passed with {len(warnings)} warnings"
    
    return result


def optimize_template_performance(template_data: Dict[str, Any]) -> str:
    """
    Optimize template for better performance and deliverability.
    
    Args:
        template_data: Template data including content and metadata
        
    Returns:
        Optimization result
    """
    template_name = template_data.get('name', 'unknown')
    html_content = template_data.get('html_content', '')
    
    print(f"⚡ Optimizing template: {template_name}")
    print(f"   Original size: {len(html_content)} characters")
    
    optimization_stats = {
        'original_size': len(html_content),
        'optimized_size': 0,
        'optimizations': []
    }
    
    # Simulate optimizations
    time.sleep(0.8)
    
    # Minify HTML (remove extra whitespace)
    if '  ' in html_content or '\n' in html_content:
        # Simulate minification
        optimized_html = re.sub(r'\s+', ' ', html_content)
        size_savings = len(html_content) - len(optimized_html)
        optimization_stats['optimizations'].append(f"HTML minification: -{size_savings} chars")
    
    # Inline CSS optimization
    if '<style>' in html_content:
        optimization_stats['optimizations'].append("CSS optimization: -15% stylesheet size")
    
    # Image optimization suggestions
    img_count = html_content.lower().count('<img')
    if img_count > 0:
        optimization_stats['optimizations'].append(f"Found {img_count} images - consider WebP format")
    
    # Link optimization
    link_count = html_content.lower().count('<a ')
    if link_count > 10:
        optimization_stats['optimizations'].append(f"High link count ({link_count}) - may affect deliverability")
    
    # Calculate final stats
    estimated_optimized_size = int(len(html_content) * 0.85)  # Estimate 15% reduction
    optimization_stats['optimized_size'] = estimated_optimized_size
    size_reduction = len(html_content) - estimated_optimized_size
    reduction_percent = (size_reduction / len(html_content)) * 100 if len(html_content) > 0 else 0
    
    print(f"   Optimized size: {estimated_optimized_size} characters")
    print(f"   Size reduction: {size_reduction} chars ({reduction_percent:.1f}%)")
    print(f"   Optimizations applied: {len(optimization_stats['optimizations'])}")
    
    for opt in optimization_stats['optimizations']:
        print(f"     • {opt}")
    
    result = f"Template {template_name} optimized: {reduction_percent:.1f}% size reduction, {len(optimization_stats['optimizations'])} optimizations"
    print(f"✅ {result}")
    
    return result


def generate_template_variations(base_template: Dict[str, Any], 
                               variations: List[Dict[str, Any]]) -> str:
    """
    Generate multiple template variations for A/B testing.
    
    Args:
        base_template: Base template configuration
        variations: List of variation specifications
        
    Returns:
        Generation result
    """
    template_name = base_template.get('name', 'unknown')
    
    print(f"🔄 Generating template variations: {template_name}")
    print(f"   Base template: {template_name}")
    print(f"   Variations to generate: {len(variations)}")
    
    generated_variations = []
    
    for i, variation in enumerate(variations):
        variation_name = f"{template_name}_var_{chr(65+i)}"  # A, B, C, etc.
        
        print(f"   🎨 Generating variation {chr(65+i)}: {variation.get('description', 'No description')}")
        
        # Simulate variation generation
        time.sleep(0.4)
        
        # Apply variation changes
        changes = []
        if 'subject_style' in variation:
            changes.append(f"Subject: {variation['subject_style']} style")
        if 'cta_color' in variation:
            changes.append(f"CTA color: {variation['cta_color']}")
        if 'layout' in variation:
            changes.append(f"Layout: {variation['layout']}")
        if 'tone' in variation:
            changes.append(f"Tone: {variation['tone']}")
        
        generated_variation = {
            'name': variation_name,
            'changes': changes,
            'expected_impact': variation.get('expected_impact', 'unknown')
        }
        
        generated_variations.append(generated_variation)
        
        print(f"     ✅ Generated {variation_name}")
        for change in changes:
            print(f"       • {change}")
    
    # Summary
    print(f"   📊 Generation complete:")
    print(f"     Base template: 1")
    print(f"     Variations: {len(generated_variations)}")
    print(f"     Total templates: {len(generated_variations) + 1}")
    
    result = f"Generated {len(generated_variations)} variations of {template_name}"
    print(f"✅ {result}")
    
    return result


def process_template_analytics(template_id: str, analytics_data: Dict[str, Any]) -> str:
    """
    Process template performance analytics.
    
    Args:
        template_id: Template identifier
        analytics_data: Performance metrics data
        
    Returns:
        Analytics processing result
    """
    print(f"📊 Processing template analytics: {template_id}")
    
    metrics = analytics_data.get('metrics', {})
    sent_count = metrics.get('sent', 0)
    delivered_count = metrics.get('delivered', 0)
    opened_count = metrics.get('opened', 0)
    clicked_count = metrics.get('clicked', 0)
    
    print(f"   📈 Performance metrics:")
    print(f"     Sent: {sent_count}")
    print(f"     Delivered: {delivered_count}")
    print(f"     Opened: {opened_count}")
    print(f"     Clicked: {clicked_count}")
    
    # Calculate rates
    if sent_count > 0:
        delivery_rate = (delivered_count / sent_count) * 100
        open_rate = (opened_count / sent_count) * 100
        click_rate = (clicked_count / sent_count) * 100
        
        print(f"   📊 Calculated rates:")
        print(f"     Delivery rate: {delivery_rate:.1f}%")
        print(f"     Open rate: {open_rate:.1f}%")
        print(f"     Click rate: {click_rate:.1f}%")
        
        # Performance assessment
        performance_score = (delivery_rate * 0.3) + (open_rate * 0.4) + (click_rate * 0.3)
        
        if performance_score >= 80:
            assessment = "Excellent"
        elif performance_score >= 60:
            assessment = "Good"
        elif performance_score >= 40:
            assessment = "Average"
        else:
            assessment = "Needs Improvement"
        
        print(f"   🏆 Performance assessment: {assessment} ({performance_score:.1f}/100)")
        
        # Generate recommendations
        recommendations = []
        if delivery_rate < 95:
            recommendations.append("Improve sender reputation or email validation")
        if open_rate < 20:
            recommendations.append("Optimize subject line and sender name")
        if click_rate < 3:
            recommendations.append("Improve call-to-action placement and content")
        
        if recommendations:
            print(f"   💡 Recommendations:")
            for rec in recommendations:
                print(f"     • {rec}")
    
    # Simulate analytics processing
    time.sleep(0.6)
    
    result = f"Analytics processed for template {template_id}: {assessment} performance"
    print(f"✅ {result}")
    
    return result


# ===== TEMPLATE RENDERING HELPERS =====

def get_localized_subject(template_type: str, language: str) -> str:
    """Get localized subject line for template type."""
    subjects = {
        'welcome': {
            'en': 'Welcome to NAQ Email System!',
            'es': '¡Bienvenido al Sistema de Email NAQ!',
            'fr': 'Bienvenue dans le système d\'email NAQ!'
        },
        'newsletter': {
            'en': 'Your Weekly NAQ Newsletter',
            'es': 'Tu Newsletter Semanal de NAQ',
            'fr': 'Votre Newsletter Hebdomadaire NAQ'
        },
        'order_confirmation': {
            'en': 'Order Confirmation',
            'es': 'Confirmación de Pedido',
            'fr': 'Confirmation de Commande'
        },
        'password_reset': {
            'en': 'Password Reset Request',
            'es': 'Solicitud de Restablecimiento de Contraseña',
            'fr': 'Demande de Réinitialisation du Mot de Passe'
        }
    }
    
    return subjects.get(template_type, {}).get(language, subjects.get(template_type, {}).get('en', 'NAQ Email'))


def render_welcome_template(context: Dict[str, Any], language: str) -> str:
    """Render welcome email template."""
    name = escape(context.get('user_name', 'Friend'))
    company = escape(context.get('company_name', 'NAQ System'))
    
    if language == 'es':
        content = f"""
        <h1>¡Hola {name}!</h1>
        <p>Bienvenido a {company}. Estamos emocionados de tenerte con nosotros.</p>
        <p>Esperamos que disfrutes usando nuestro servicio.</p>
        """
    elif language == 'fr':
        content = f"""
        <h1>Bonjour {name}!</h1>
        <p>Bienvenue chez {company}. Nous sommes ravis de vous avoir avec nous.</p>
        <p>Nous espérons que vous apprécierez l'utilisation de notre service.</p>
        """
    else:  # English
        content = f"""
        <h1>Hello {name}!</h1>
        <p>Welcome to {company}. We're excited to have you with us.</p>
        <p>We hope you'll enjoy using our service.</p>
        """
    
    return f"""
    <html>
    <body>
        {content}
        <p><a href="{context.get('unsubscribe_url')}">Unsubscribe</a></p>
        <img src="{context.get('tracking_pixel')}" width="1" height="1" alt="">
    </body>
    </html>
    """


def render_newsletter_template(context: Dict[str, Any], language: str) -> str:
    """Render newsletter template."""
    return f"""
    <html>
    <body>
        <h1>Newsletter</h1>
        <p>Latest updates from {escape(context.get('company_name', 'NAQ'))}.</p>
        <p><a href="{context.get('unsubscribe_url')}">Unsubscribe</a></p>
        <img src="{context.get('tracking_pixel')}" width="1" height="1" alt="">
    </body>
    </html>
    """


def render_order_template(context: Dict[str, Any], language: str) -> str:
    """Render order confirmation template."""
    order_id = escape(str(context.get('order', {}).get('id', 'N/A')))
    total = context.get('total', 0)
    
    return f"""
    <html>
    <body>
        <h1>Order Confirmation</h1>
        <p>Order ID: {order_id}</p>
        <p>Total: ${total:.2f}</p>
        <p>Thank you for your order!</p>
        <p><a href="{context.get('unsubscribe_url')}">Unsubscribe</a></p>
        <img src="{context.get('tracking_pixel')}" width="1" height="1" alt="">
    </body>
    </html>
    """


def render_password_reset_template(context: Dict[str, Any], language: str) -> str:
    """Render password reset template."""
    name = escape(context.get('user_name', 'Friend'))
    
    return f"""
    <html>
    <body>
        <h1>Password Reset</h1>
        <p>Hello {name},</p>
        <p>Click the link below to reset your password:</p>
        <p><a href="#reset-link">Reset Password</a></p>
        <p><a href="{context.get('unsubscribe_url')}">Unsubscribe</a></p>
        <img src="{context.get('tracking_pixel')}" width="1" height="1" alt="">
    </body>
    </html>
    """


def render_generic_template(template_name: str, context: Dict[str, Any], language: str) -> str:
    """Render generic template."""
    return f"""
    <html>
    <body>
        <h1>{escape(template_name.replace('_', ' ').title())}</h1>
        <p>Email content goes here.</p>
        <p><a href="{context.get('unsubscribe_url')}">Unsubscribe</a></p>
        <img src="{context.get('tracking_pixel')}" width="1" height="1" alt="">
    </body>
    </html>
    """


def html_to_text(html_content: str) -> str:
    """Convert HTML to plain text."""
    # Simple HTML to text conversion (in production, use a proper library)
    text = re.sub(r'<[^>]+>', '', html_content)
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def sanitize_html_content(html_content: str) -> str:
    """Sanitize HTML content for security."""
    # Remove dangerous tags and attributes
    html_content = re.sub(r'<script.*?</script>', '', html_content, flags=re.IGNORECASE | re.DOTALL)
    html_content = re.sub(r'javascript:', '', html_content, flags=re.IGNORECASE)
    return html_content


def sanitize_text_content(text_content: str) -> str:
    """Sanitize text content."""
    return text_content.strip()


# ===== DEMONSTRATION FUNCTIONS =====

def demonstrate_template_rendering():
    """Demonstrate template rendering capabilities."""
    print("📍 Template Rendering Demo")
    print("-" * 40)
    
    with SyncClient() as client:
        jobs = []
        
        # Test different templates and languages
        templates = [
            {
                'name': 'welcome',
                'context': {'user_name': 'Alice Johnson'},
                'language': 'en',
                'preferences': {'timezone': 'EST', 'frequency': 'weekly'}
            },
            {
                'name': 'newsletter',
                'context': {'user_name': 'Carlos Rodriguez'},
                'language': 'es',
                'preferences': {'timezone': 'PST', 'frequency': 'daily'}
            },
            {
                'name': 'order_confirmation',
                'context': {
                    'user_name': 'Marie Dubois',
                    'order': {'id': 'ORD-12345'},
                    'total': 89.99
                },
                'language': 'fr',
                'preferences': {'timezone': 'CET', 'frequency': 'monthly'}
            }
        ]
        
        print("📤 Rendering templates...")
        for template in templates:
            job = client.enqueue(
                render_email_template,
                template_name=template['name'],
                context=template['context'],
                language=template['language'],
                user_preferences=template['preferences'],
                queue_name="email_template"
            )
            jobs.append(job)
            print(f"  ✅ {template['name']} ({template['language']}): {job.job_id}")
        
        return jobs


def demonstrate_template_validation():
    """Demonstrate template validation functionality."""
    print("\n📍 Template Validation Demo")
    print("-" * 40)
    
    with SyncClient() as client:
        jobs = []
        
        # Test templates with various content
        test_templates = [
            {
                'content': '<html><body><h1>Safe Template</h1><p>Hello {{name}}!</p><a href="{{unsubscribe_url}}">Unsubscribe</a></body></html>',
                'type': 'html'
            },
            {
                'content': '<html><body><script>alert("xss")</script><h1>Unsafe Template</h1></body></html>',
                'type': 'html'
            },
            {
                'content': 'This is a very long subject line that exceeds the recommended length for mobile devices and may be truncated',
                'type': 'subject'
            }
        ]
        
        print("📤 Validating templates...")
        for i, template in enumerate(test_templates):
            job = client.enqueue(
                validate_template_content,
                template_content=template['content'],
                template_type=template['type'],
                queue_name="email_template"
            )
            jobs.append(job)
            print(f"  ✅ Template {i+1} ({template['type']}): {job.job_id}")
        
        return jobs


def demonstrate_template_optimization():
    """Demonstrate template optimization features."""
    print("\n📍 Template Optimization Demo")
    print("-" * 40)
    
    with SyncClient() as client:
        jobs = []
        
        # Templates to optimize
        templates_to_optimize = [
            {
                'name': 'newsletter_v1',
                'html_content': '''
                <html>
                <head>
                    <style>
                        body { font-family: Arial, sans-serif; }
                        .header { background-color: #blue; }
                        .content { padding: 20px; }
                    </style>
                </head>
                <body>
                    <div class="header">
                        <h1>Newsletter</h1>
                    </div>
                    <div class="content">
                        <p>This is newsletter content with lots of spacing and formatting.</p>
                        <img src="image1.jpg" />
                        <img src="image2.jpg" />
                        <a href="link1">Link 1</a>
                        <a href="link2">Link 2</a>
                    </div>
                </body>
                </html>
                ''',
                'metadata': {'created': '2024-01-01', 'version': 1}
            }
        ]
        
        print("📤 Optimizing templates...")
        for template in templates_to_optimize:
            job = client.enqueue(
                optimize_template_performance,
                template_data=template,
                queue_name="email_template"
            )
            jobs.append(job)
            print(f"  ✅ {template['name']}: {job.job_id}")
        
        return jobs


def demonstrate_template_variations():
    """Demonstrate A/B test template generation."""
    print("\n📍 Template Variations Demo")
    print("-" * 40)
    
    with SyncClient() as client:
        jobs = []
        
        base_template = {
            'name': 'promotional_email',
            'subject': 'Special Offer Inside!',
            'content': 'Basic promotional email content'
        }
        
        variations = [
            {
                'description': 'Urgency-focused variant',
                'subject_style': 'urgent',
                'cta_color': 'red',
                'expected_impact': 'higher_clicks'
            },
            {
                'description': 'Personal touch variant', 
                'subject_style': 'personal',
                'layout': 'minimal',
                'tone': 'friendly',
                'expected_impact': 'higher_opens'
            },
            {
                'description': 'Product-focused variant',
                'subject_style': 'descriptive',
                'layout': 'product_grid',
                'cta_color': 'green',
                'expected_impact': 'higher_conversions'
            }
        ]
        
        print("📤 Generating template variations...")
        job = client.enqueue(
            generate_template_variations,
            base_template=base_template,
            variations=variations,
            queue_name="email_template"
        )
        jobs.append(job)
        print(f"  ✅ A/B variations for {base_template['name']}: {job.job_id}")
        
        return jobs


def demonstrate_template_analytics():
    """Demonstrate template analytics processing."""
    print("\n📍 Template Analytics Demo")
    print("-" * 40)
    
    with SyncClient() as client:
        jobs = []
        
        # Simulate analytics for different templates
        analytics_scenarios = [
            {
                'template_id': 'welcome_v1',
                'metrics': {'sent': 1000, 'delivered': 980, 'opened': 245, 'clicked': 49}
            },
            {
                'template_id': 'newsletter_v2',
                'metrics': {'sent': 5000, 'delivered': 4850, 'opened': 970, 'clicked': 145}
            },
            {
                'template_id': 'promo_urgent',
                'metrics': {'sent': 2500, 'delivered': 2375, 'opened': 380, 'clicked': 76}
            }
        ]
        
        print("📤 Processing template analytics...")
        for scenario in analytics_scenarios:
            job = client.enqueue(
                process_template_analytics,
                template_id=scenario['template_id'],
                analytics_data=scenario,
                queue_name="email_template"
            )
            jobs.append(job)
            print(f"  ✅ Analytics for {scenario['template_id']}: {job.job_id}")
        
        return jobs


def main():
    """
    Main function demonstrating template processing capabilities.
    """
    print("🎨 NAQ Template Processor Demo")
    print("=" * 50)
    
    # Check configuration
    serializer = os.environ.get('NAQ_JOB_SERIALIZER', 'pickle')
    if serializer != 'json':
        print("❌ SECURITY WARNING: Not using JSON serialization!")
        print("   Set: export NAQ_JOB_SERIALIZER=json")
        return 1
    else:
        print("✅ Using secure JSON serialization")
    
    try:
        # Run demonstrations
        rendering_jobs = demonstrate_template_rendering()
        validation_jobs = demonstrate_template_validation()
        optimization_jobs = demonstrate_template_optimization()
        variation_jobs = demonstrate_template_variations()
        analytics_jobs = demonstrate_template_analytics()
        
        # Summary
        total_jobs = (len(rendering_jobs) + len(validation_jobs) + len(optimization_jobs) + 
                     len(variation_jobs) + len(analytics_jobs))
        
        print(f"\n🎉 Template processor demo completed!")
        print(f"📊 Enqueued {total_jobs} jobs total:")
        print(f"   🎨 {len(rendering_jobs)} template renderings")
        print(f"   🔍 {len(validation_jobs)} template validations")
        print(f"   ⚡ {len(optimization_jobs)} template optimizations")
        print(f"   🔄 {len(variation_jobs)} A/B variation generations")
        print(f"   📊 {len(analytics_jobs)} analytics processing")
        
        print("\n🛠️  Template Processing Features Demonstrated:")
        print("   • Multi-language template rendering")
        print("   • Dynamic context and personalization")
        print("   • Template validation and security scanning")
        print("   • Performance optimization")
        print("   • A/B test variation generation")
        print("   • Template analytics and recommendations")
        print("   • HTML to text conversion")
        print("   • Content sanitization")
        
        print("\n💡 Watch your worker logs to see:")
        print("   - Template rendering with different languages")
        print("   - Security validation catching dangerous content")
        print("   - Performance optimizations being applied")
        print("   - A/B variations being generated")
        print("   - Analytics insights and recommendations")
        
        print("\n📋 Next Steps:")
        print("   - Implement real template engine (Jinja2, Handlebars)")
        print("   - Add template versioning and rollback")
        print("   - Create visual template editor")
        print("   - Implement template A/B testing automation")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        print("\n🔧 Troubleshooting:")
        print("   - Is NATS running? (cd docker && docker-compose up -d)")
        print("   - Are workers running? (naq worker default email_template)")
        print("   - Is NAQ_JOB_SERIALIZER=json set?")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())