%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for encoding/decoding models implementing
%%% 'jsonable' behaviour.
%%% @end
%%%-------------------------------------------------------------------
-module(json_serializer).
-author("Bartosz Walkowicz").

-include_lib("ctool/include/logging.hrl").

%% API
-export([encode/1, decode/1]).


-type model_name() :: module().
-type model_version() :: non_neg_integer().
-type model_record() :: tuple().

-export_type([model_name/0, model_version/0, model_record/0]).


%%%===================================================================
%%% API functions
%%%===================================================================


-spec encode(model_record()) -> binary().
encode(SerializableRecord) ->
    ModelName = utils:record_type(SerializableRecord),

    json_utils:encode(#{
        <<"_recordType">> => atom_to_binary(ModelName, utf8),
        <<"_version">> => ModelName:version(),
        <<"_data">> => ModelName:to_json(SerializableRecord)
    }).


-spec decode(binary()) -> model_record().
decode(EncodedRecord) ->
    #{
        <<"_recordType">> := ModelBin,
        <<"_version">> := CurrentModelVersion,
        <<"_data">> := RecordJson
    } = json_utils:decode(EncodedRecord),

    ModelName = binary_to_existing_atom(ModelBin, utf8),
    TargetModelVersion = ModelName:version(),

    UpgradedRecordData = upgrade_record_json(
        TargetModelVersion, CurrentModelVersion, ModelName, RecordJson
    ),

    ModelName:from_json(UpgradedRecordData).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec upgrade_record_json(model_version(), model_version(), model_name(), json_utils:json_map()) ->
    json_utils:json_map() | no_return().
upgrade_record_json(Version, Version, _ModelName, Json) ->
    Json;

upgrade_record_json(TargetVersion, CurrentVersion, ModelName, _Json) when CurrentVersion > TargetVersion ->
    ?emergency(
        "Upgrade requested for json ModelName '~p' with future version ~p (known versions up to: ~p)",
        [ModelName, CurrentVersion, TargetVersion]
    ),
    error({future_version, ModelName, CurrentVersion, TargetVersion});

upgrade_record_json(TargetVersion, CurrentVersion, ModelName, Json) ->
    {NewVersion, Json2} = ModelName:upgrade_json(CurrentVersion, Json),
    upgrade_record_json(TargetVersion, NewVersion, ModelName, Json2).
