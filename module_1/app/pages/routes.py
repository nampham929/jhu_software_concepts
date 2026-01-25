from flask import Blueprint, render_template

# Instantiate blueprint object
bp = Blueprint(
    "pages",
    __name__,
    template_folder="templates"
)


# Render Home page
@bp.route("/")
def home():
    return render_template("home.html", active_page="home", title="Home")


# Render Projects age
@bp.route("/projects")
def projects():
    return render_template("projects.html", active_page="projects", title="Projects")


# Redern Contact page
@bp.route("/contact")
def contact():
    return render_template("contact.html", active_page="contact", title="Contact")