App.FileTreeNodeComponent = Ember.Component.extend({
    actions: {
        clickAction: function (file) {
            this.sendAction('clickAction', file);
        },

        // TODO components are nested, so we need to bubble the action up
        // until it reaches the controller
        fileClicked: function (file) {
            this.sendAction('clickAction', file);
        }
    }
});