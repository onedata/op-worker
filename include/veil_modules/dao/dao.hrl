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

-record(some_record, {field1="", field2="", field3=""}).


-define(dao_record_info(R),
    case R of
        some_record -> {record_info(size, some_record), record_info(fields, some_record)};
        %next_record -> record_info(fields, next_record);
        _ -> {error, unsupported_record}
    end).



-define(SYSTEM_DB_NAME, "system_data").
-define(RECORD_INSTANCES_DOC_PREFIX, "record_instances_").