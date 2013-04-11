%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: dao header
%% @end
%% ===================================================================

-include_lib("records.hrl").

%% record definition used in record registration example
-record(some_record, {field1 = "", field2 = "", field3 = ""}).

%% Helper macro. See macro ?dao_record_info/1 for more details.
-define(record_info_gen(X), {record_info(size, X), record_info(fields, X), #X{}}).

%% Every record that will be saved to DB have to be "registered" with this define.
%% Each registered record should be listed in defined below 'case' block as fallow:
%% record_name -> ?record_info_gen(record_name);
%% when 'record_name' is name of the record. 'some_record' is an example.
-define(dao_record_info(R),
    case R of
        some_record -> ?record_info_gen(some_record);
        cm_state -> ?record_info_gen(cm_state);
        host_state -> ?record_info_gen(host_state);
        node_state -> ?record_info_gen(node_state);
        %next_record -> ?record_info_gen(next_record);
        _ -> {error, unsupported_record}
    end).


%% DB constants
-define(SYSTEM_DB_NAME, "system_data").
-define(RECORD_INSTANCES_DOC_PREFIX, "record_instances_").
-define(RECORD_FIELD_BINARY_PREFIX, "__bin__: ").
-define(RECORD_FIELD_ATOM_PREFIX, "__atom__: ").
-define(RECORD_FIELD_PID_PREFIX, "__pid__: ").
-define(RECORD_TUPLE_FIELD_NAME_PREFIX, "tuple_field_").
-define(RECORD_META_FIELD_NAME, "record__").