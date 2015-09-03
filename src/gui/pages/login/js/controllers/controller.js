Login.LoginFormController = Ember.ObjectController.extend({
    username: null,
    loginFailed: null,

    actions: {
        login: function () {
            $.post("/validate_login.html", {
                username: this.get("username")
            }).then(function () {
                document.location = "/file_manager.html";
            }, function () {
                this.set("loginFailed", true);
            }.bind(this));
        }
    }
});
