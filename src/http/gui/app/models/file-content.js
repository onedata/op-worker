/**
 * This is a prototype model representing file content in file browser.
 * @module models/file-content
 * @author Łukasz Opioła
 * @copyright (C) 2016 ACK CYFRONET AGH
 * @license This software is released under the MIT license cited in 'LICENSE.txt'.
 */

import DS from 'ember-data';

export default DS.Model.extend({
  bytes: DS.attr('string'),
  file: DS.belongsTo('file')
});
