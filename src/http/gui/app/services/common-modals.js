import Ember from 'ember';
import snakeToCamel from '../utils/snake-to-camel';

export default Ember.Service.extend({
  component: null,

  openModal(type, params) {
    this.set('component.modalParams', params);
    this.set('component.' + snakeToCamel('opened-' + type), true);
  }
});
