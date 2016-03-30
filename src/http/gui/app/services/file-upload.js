import Ember from 'ember';

export default Ember.Service.extend({
  fileUploadComponent: null,

  assignDrop(jqDropElement) {
    this.get('fileUploadComponent').assignDrop(jqDropElement);
  },

  assignBrowse(jqBrowseElement) {
    this.get('fileUploadComponent').assignBrowse(jqBrowseElement);
  }
});
