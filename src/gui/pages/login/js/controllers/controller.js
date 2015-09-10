Login.LoginFormController = Ember.ObjectController.extend({
    username: null,
    loginFailed: null,

    actions: {
        login: function () {
            $.post("/ver_login.html", {
                username: this.get("username")
            }).then(function () {
                console.log('ok');
            }, function () {
                this.set("loginFailed", true);
            }.bind(this));
        }
    }
});
