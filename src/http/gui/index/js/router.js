App.Router.map(function() {
    //this.route('main', { path: '/' });
    this.route('main', { path: '/' });
    //this.route('global', { path: '/*' });
});

App.MainRoute = Ember.Route.extend({
    model: function () {
        return this.store.find('file');
    },
    actions: {
        showModal: function(name, controller, model) {
            this.render(name, {
                into: 'main',
                outlet: 'modal',
                controller: controller,
                model: model
            });
        },
        hideModal: function() {
            this.disconnectOutlet({
                outlet: 'modal',
                parentView: 'main'
            });
        }
    }
});
