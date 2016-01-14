// Prototype ember controller for the component top-menu.
// Here user actions are handled.

App.TopMenuComponent = Ember.Component.extend({
    actions: {
        clickAction: function () {
            console.log('clickAction');
            this.sendAction('clickAction');
        }
    }
});