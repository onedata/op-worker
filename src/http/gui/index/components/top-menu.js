App.TopMenuComponent = Ember.Component.extend({
    actions: {
        clickAction: function () {
            console.log('clickAction');
            this.sendAction('clickAction');
        }
    }
});