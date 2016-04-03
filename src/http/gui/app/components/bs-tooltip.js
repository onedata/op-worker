import Ember from 'ember';

export default Ember.Component.extend({
  attributeBindings: ['dataToggle:data-toggle', 'placement:data-placement'],
  dataToggle: 'tooltip',
  // default
  placement: 'bottom',
  title: null,

  bootstrapize: function() {
    if (this.get('title')) {
      this.$().tooltip('hide')
          .attr('data-original-title', this.get('title'))
          .tooltip('fixTitle');
    }
  }.observes('title'),

  didInsertElement() {
    this.bootstrapize();
  }
});
