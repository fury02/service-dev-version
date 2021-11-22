import ExperimentalCycles "mo:base/ExperimentalCycles";
import Debug "mo:base/Debug";
import Principal "mo:base/Principal";
import Text "mo:base/Text";
import HashMap "mo:base/HashMap";
import Array "mo:base/Array";
import Nat "mo:base/Nat";
import List "mo:base/List";
import Iter "mo:base/Iter";
import Prim "mo:prim";
import H "util/helpers";
import B "buckets";
shared ({caller = owner}) actor class DIFI_SERVICE() = this {
    private let maxBuckets: Nat = 100;
    private let freezingThreshold = 604800; // 7 day
    private var cyclesSavings: Nat = 0;//cycles  
    private let sumCreating = 200_000_000_000; //cycles
 
    private let sumFirst = 10_000_000_000_000; //cycles  
    private let cyclesCapacity: Nat = 20_000_000_000_000;//cycles  
    private let computeAllocation = 25; //25%
    private let memoryAllocation = 536870912; //512 Mb 
    private let freeSpaceBucketMemory = 10_485_760; // 10 Mb

    private flexible var buckets : [var ?B.Bucket] = Array.init(maxBuckets, null);
    //**Size service**//
    public func get_rts_memory_size(): async Nat {  
        return Prim.rts_memory_size();
    };
    //**ADD OR UPDATE**//
    public func replace_value(
      table_key: Text, 
      column_key_name: Text, 
      entity_key: Text, 
      entity_value: Text): async ?Text{  
          let (bc, _) = await get_bucket_key_contains(table_key, entity_key);
          switch(bc){
            case(null){
              let fb: ?B.Bucket = await get_bucket();
              switch(fb){
                case(?fb){ return await fb.replace_value(table_key, column_key_name, entity_key, entity_value);};
                case(null){ return null;};
              };
            };
            case(?bc){ return await bc.replace_value(table_key, column_key_name, entity_key, entity_value);};
          }; 
    };
    //**FIND**//
    public func find_table_cell(
      table_key: Text, 
      column_key_name: Text, 
      entity_key: Text): async Text{
        var result = "null";
        let (bc, _) = await get_bucket_key_contains(table_key, entity_key);
        switch(bc){
          case(null){
            return result;
          };
          case(?bc){
            let v = await bc.find_table_cell(table_key, column_key_name, entity_key);
            return v;
          };
        }; 
    };
    public func find_table_value(
      table_key: Text, 
      entity_key: Text): async Text{
        let (bc, _) = await get_bucket_key_contains(table_key, entity_key);
        switch(bc){
          case(null){
            return "{}";
          };
          case(?bc){
            let v = await bc.find_table_value(table_key, entity_key);
            return v;
          };
        }; 
    };
    //collection//
    public func get_table_entityes(
      table_key: Text): async [Text]{
      let l: List.List<B.Bucket> = await get_buckets_table_contains(table_key);
      var hm = HashMap.HashMap<Text,Text>(0, Text.equal, Text.hash);
      var a : [var Text] = Array.init(0, "");
      let fa: [Text] = Array.freeze<Text>(a); 
      if(List.isNil(l)){
        return fa;
      }
      else{
        let vl: [B.Bucket] = List.toArray<B.Bucket>(l);
        for(b in vl.vals()){
          let c = await b.get_collection_table_entityes(table_key);
            for(v in c.vals()){
              var inc = hm.size() + 1; 
              let r = hm.replace(Nat.toText(inc), v);
          };
        };
        if(Nat.equal(hm.size(),0)){
          return fa;
        }else{
          var i = 0;
          a := Array.init(hm.size(), "");
          for((k,v) in hm.entries()){
            a[i] := v;
            i := i + 1;
          };
          let fa_: [Text] = Array.freeze<Text>(a); 
          return fa_;
        };
      };  
      return fa;
    };
    //json//
    public func get_table_entityes_json(
      table_key: Text): async Text{
      let l: List.List<B.Bucket> = await get_buckets_table_contains(table_key);
      var result = "";
      if(List.isNil(l)){
        return "[{}]";
      }
      else{
        let a: [B.Bucket] = List.toArray<B.Bucket>(l);
        for(b in a.vals()){
          let c = await b.get_collection_table_entityes(table_key);
            for(v in c.vals()){
              result := H.text_concat(v, result, ", ");  
            };
        };
        result := Text.trimEnd(result, #char ' ');
        result := Text.trimEnd(result, #char ','); 
        result := Text.concat("[", result);
        result := Text.concat(result, "]");
      };  
      return result;
    };
    //collection//
    public func get_table_keys(
      table_key: Text): async [Text]{
        let l: List.List<B.Bucket> = await get_buckets_table_contains(table_key);
        var hm = HashMap.HashMap<Text,Text>(0, Text.equal, Text.hash);
        var a : [var Text] = Array.init(0, "");
        let fa: [Text] = Array.freeze<Text>(a); 
        if(List.isNil(l)){
            return fa;
        }
        else{
            let vl: [B.Bucket] = List.toArray<B.Bucket>(l);
            for(b in vl.vals()){
                let c = await b.get_collection_table_keys(table_key);
                for(k in c.vals()){
                    let i = hm.replace(k, k);
                };
            };
            if(Nat.equal(hm.size(),0)){
              return fa;
            }else{
              var i = 0;
              a := Array.init(hm.size(), "");
              for((k,v) in hm.entries()){
                a[i] := k;
                i := i + 1;
              };
              let fa_: [Text] = Array.freeze<Text>(a); 
              return fa_;
            };
        };  
    };
    //json//
    public func get_table_keys_json(
      table_key: Text): async Text{
        var result = "";
        let sign = "\"";
        let strt = "Key";
        let l: List.List<B.Bucket> = await get_buckets_table_contains(table_key);
        if(List.isNil(l)){
            return "[{}]";
        }
        else{
            let a: [B.Bucket] = List.toArray<B.Bucket>(l);
            for(b in a.vals()){
                let c = await b.get_collection_table_keys(table_key);
                for(v in c.vals()){
                    var ck = H.text_concat(sign, sign, v);
                    var concat = H.text_concat(strt, ck, " : ");
                    concat := H.text_concat("{", "}", concat);
                    result := H.text_concat(concat, result, ", ");  
                };
            };
          result := Text.trimEnd(result, #char ' ');
          result := Text.trimEnd(result, #char ',');  
          result := Text.concat("[", result);
          result := Text.concat(result, "]");  
        };  
        return result;
    };
    //collection//
    public func get_tables(): async [Text]{
        var hm = HashMap.HashMap<Text,Text>(0, Text.equal, Text.hash);
        var a : [var Text] = Array.init(0, "");
        let fa: [Text] = Array.freeze<Text>(a); 
        for(b in buckets.vals()){
            switch(b){
                case(null){};
                case(?b){
                    let c = await b.get_collection_tables();
                    for(k in c.vals()){
                        let i = hm.replace(k, k);
                    };
                };
            };
        };
        if(Nat.equal(hm.size(),0)){
            return fa;
        }else{
            var i = 0;
            a := Array.init(hm.size(), "");
            for((k,v) in hm.entries()){
                a[i] := k;
                i := i + 1;
            };
            let fa_: [Text] = Array.freeze<Text>(a); 
            return fa_;
        }; 
    };
    //json//
    public func get_tables_json(): async Text{
        var hm = HashMap.HashMap<Text,Text>(0, Text.equal, Text.hash);
        var result = "";
        let sign = "\"";
        let strt = "Table";  
        for(b in buckets.vals()){
            switch(b){
                case(null){};
                case(?b){
                    let a = await b.get_collection_tables();
                    for(k in a.vals()){
                        let i = hm.replace(k, k);
                    };
                };
            };
        };
        if(Nat.equal(hm.size(),0)){
                return "[{}]";
        }else{
            for((k,v) in hm.entries()){
                var ck = H.text_concat(sign, sign, v);
                var concat = H.text_concat(strt, ck, " : ");
                concat := H.text_concat("{", "}", concat);
                result := H.text_concat(concat, result, ", "); 
            };
            result := Text.trimEnd(result, #char ' ');
            result := Text.trimEnd(result, #char ',');  
            result := Text.concat("[", result);
            result := Text.concat(result, "]");
            return result;
        }; 
    };
    //**DELETE**//
    public func delete_table_cell_value(
      table_key: Text, 
      column_key_name: Text, 
      entity_key: Text): async Bool{
        let (b, _) = await get_bucket_key_contains(table_key, entity_key);
        switch(b){
            case(null){
                return false;
            };
            case(?b){
                let v = await b.delete_table_cell_value(table_key, column_key_name, entity_key);
                return v;
            };
        };
    };
    public func delete_table_entity(
      table_key: Text, 
      entity_key: Text): async Bool{
        let (b, _) = await get_bucket_key_contains(table_key, entity_key);
        switch(b){
            case(null){
                return false;
            };
            case(?b){
                let v = await b.delete_table_entity(table_key, entity_key);
                return v;
            };
        };
    };
    public func delete_table(
      table_key: Text): async Bool{
        let l: List.List<B.Bucket> = await get_buckets_table_contains(table_key);
        var bl = false;
        if(List.isNil(l)){
            return false;
        }
        else{
            let a: [B.Bucket] = List.toArray<B.Bucket>(l);
            for(b in a.vals()){
                bl := true;
                var r = await b.delete_table(table_key);
            };
        };  
        return bl; 
    };
    public func delete_column(
      table_key: Text,
      column_key_name: Text): async Bool{
        let l: List.List<B.Bucket> = await get_buckets_table_contains(table_key);
        var bl = false;
        if(List.isNil(l)){
            return false;
        }
        else{
            let a: [B.Bucket] = List.toArray<B.Bucket>(l);
            for(b in a.vals()){
                bl := await b.delete_column(table_key, column_key_name);
            };
        };  
        return bl; 
    };
    //**CLEAR**//
    public func clear_table(
      table_key: Text): async Bool{
        let l: List.List<B.Bucket> = await get_buckets_table_contains(table_key);
        var bl = false;
        if(List.isNil(l)){
            return false;
        }
        else{
            let a: [B.Bucket] = List.toArray<B.Bucket>(l);
            for(b in a.vals()){
              bl := await b.clear_table(table_key);
            };
        };  
        return bl; 
    };
    public func clear_column(
      table_key: Text,
      column_key_name: Text): async Bool{
        let l: List.List<B.Bucket> = await get_buckets_table_contains(table_key);
        var bl = false;
        if(List.isNil(l)){
            return false;
        }
        else{
            let a: [B.Bucket] = List.toArray<B.Bucket>(l);
            for(b in a.vals()){
              bl := await b.clear_column(table_key, column_key_name);
            };
        };  
        return bl; 
    };
    //**OTHER**//
    public func exist_table(
        table_key: Text): async Bool{
        let l: List.List<B.Bucket> = await get_buckets_table_contains(table_key);
        if(List.isNil(l)){
          return false;
        }
        else{
          return true;
        };  
    };
    //**Bucket operation**// 
    //**Create and set settings for bucket**// 
    private func create_bucket(): async ?B.Bucket{
      ExperimentalCycles.add(sumFirst+sumCreating);
      let b: B.Bucket = await B.Bucket(this.credit);
      await b.deposit(); //deposit 
      let p: Principal = Principal.fromActor(b);
      await set_settings(p);
      return ?b;
    };
    //**Get bucket and/or create bucket**// 
    private func get_bucket(): async ?B.Bucket {
      for(b: ?B.Bucket in buckets.vals()){
          switch(b){
            case(null) {
              let bc: ?B.Bucket = await create_bucket();
              let r = add_bucket(bc);
              switch(r){
                case(false){return null};
                case(true){return bc};
              };
            };
            case(?b){
              let size_b = await b.get_rts_memory_size();
              Debug.print("service: get_bucket() info:" # debug_show("principal:", Principal.fromActor(b), "size bucket:", size_b));
              if(size_b + freeSpaceBucketMemory <  memoryAllocation)//Free balance = freeSpaceBucketMemory
              { return ?b;};
          };
        };       
      };
      return null;
    };
    //**We get a bucket if there is a key there**// 
    private func get_bucket_key_contains(
      table_key: Text,
      entity_key: Text): async (?B.Bucket, Bool) {
      for(b: ?B.Bucket in buckets.vals()){
          switch(b){
            case(null){ };
            case(?b){       
              let kc: Bool = await b.key_contains(table_key, entity_key);
              if(kc){
                return (?b, true);
              };
            };
          };       
        };
      return (null, false);
    };
    //**We get a buckets if there is a given table there**// 
    private func get_buckets_table_contains(
      table_key: Text): async List.List<B.Bucket>{
        var l : List.List<B.Bucket> = List.nil<B.Bucket>();
        for(b: ?B.Bucket in buckets.vals()){
          switch(b){
            case(null){ };
            case(?b){       
              let kc: Bool = await b.table_contains(table_key);
              if(kc){
                 l := List.push<B.Bucket>(b, l); 
              };
            };
          };       
        };
        return l;
    };
    //**Add new bucket in collections buckets**//
    private func add_bucket(bn: ?B.Bucket): Bool{
      var i = 0;
      for(b in buckets.vals()){   
        switch(b){
            case(null) {
              buckets[i] := bn;
              return true;
            };
            case(?b){
              i += 1;
          };
        };
      };
      return false;
    };
    //**Print**//
    public func print_buckets(): async(){
        for(b in buckets.vals()){
            switch(b){
                case(null){ Debug.print("bucket:" # debug_show("null")); };
                case(?b){  
                    Debug.print("bucket:" # debug_show(Principal.fromActor(b), "size: ", await b.get_rts_memory_size()));
                };
            };
        };
    };
    //**Set settings for actor**//
    private func set_settings(canister_id: Principal) : async(){
      let settings: canister_settings = { 
                controllers = ?[owner, Principal.fromActor(this)];
                compute_allocation = ?computeAllocation;
                memory_allocation = ?memoryAllocation;  
                freezing_threshold = ?freezingThreshold };
      // Debug.print("service:, set_settings() settings:" # debug_show(settings));
      await IC.update_settings({canister_id : Principal; settings : canister_settings;});
    };
    // //**Update settings for actor**//
    // public shared(msg) func update_settings(id: Text) : async(){
    //   assert (msg.caller == owner);
    //   let settings: canister_settings = { 
    //             controllers = ?[owner, Principal.fromActor(this)];
    //             compute_allocation = ?computeAllocation;
    //             memory_allocation = ?memoryAllocation;  
    //             freezing_threshold = ?freezingThreshold };
    //   Debug.print("service:, update_settings() settings:" # debug_show(settings));
    //   let canister_id = Principal.fromText(id);
    //   await IC.update_settings({canister_id: Principal; settings : canister_settings;});
    // };
    //**Manager cycles**//
    public func deposit() : async() {
      let amount = ExperimentalCycles.available();
      let limit : Nat = cyclesCapacity - cyclesSavings;
      let acceptable =
          if (amount <= limit) amount
          else limit;    
      let accepted = ExperimentalCycles.accept(acceptable);
      assert (accepted == acceptable);
      cyclesSavings += acceptable;
    };
    public func credit() : async () {
      let available = ExperimentalCycles.available();
      let accepted = ExperimentalCycles.accept(available);
      assert (accepted == available);
    };
    //**Cycles**//
    public func cycles_savings(): async Nat{
      return cyclesSavings;
    };
    public func cycles_available(): async Nat{
      return ExperimentalCycles.available();
    };
    public func cycles_balance(): async Nat{
      return ExperimentalCycles.balance();
    };
    //**Status**//
    public func canister_status(id: Text): async canister_status_type{
        let canister_id: canister_id = Principal.fromText(id);
        return await IC.canister_status({canister_id: canister_id});
    };
    //**Types for actor "aaaaa-aa"**//
    public type canister_id = Principal;
    public type canister_settings = {
        freezing_threshold : ?Nat;
        controllers : ?[Principal];
        memory_allocation : ?Nat;
        compute_allocation : ?Nat;
    };
    public type definite_canister_settings = {
        freezing_threshold : Nat;
        controllers : [Principal];
        memory_allocation : Nat;
        compute_allocation : Nat;
    };
    public type wasm_module = [Nat8];
    public type canister_status_type = {
      status : { #stopped; #stopping; #running };
      memory_size : Nat;
      cycles : Nat;
      settings : definite_canister_settings;
      module_hash : ?[Nat8];
    };
    //**Actor "aaaaa-aa"**//
    let IC = actor "aaaaa-aa" : actor {
    canister_status : shared { canister_id : canister_id } -> async {
      status : { #stopped; #stopping; #running };
      memory_size : Nat;
      cycles : Nat;
      settings : definite_canister_settings;
      module_hash : ?[Nat8];
    };
    create_canister : shared {} -> async {
      canister_id : canister_id;
    };
    delete_canister : shared { canister_id : canister_id } -> async ();
    deposit_cycles : shared { canister_id : canister_id } -> async ();
    install_code : shared {
        arg : [Nat8];
        wasm_module : wasm_module;
        mode : { #reinstall; #upgrade; #install };
        canister_id : canister_id;
      } -> async ();
    provisional_create_canister_with_cycles : shared {
        settings : ?canister_settings;
        amount : ?Nat;
      } -> async { canister_id : canister_id };
    provisional_top_up_canister : shared {
        canister_id : canister_id;
        amount : Nat;
      } -> async ();
    raw_rand : shared () -> async [Nat8];
    start_canister : shared { canister_id : canister_id } -> async ();
    stop_canister : shared { canister_id : canister_id } -> async ();
    uninstall_code : shared { canister_id : canister_id } -> async ();
    update_settings : shared {
        canister_id : Principal;
        settings : canister_settings;
      } -> async ();
    };
    //**Web ui**//
    public func ui_service_canister_id(): async Text{
       let p = Principal.fromActor(this);
       let t = Principal.toText(p);
       return t;
    };
    public func ui_service_max_buckets(): async Text{
       let r = Nat.toText(maxBuckets);
       return r;
    }; 
    public func ui_service_freezing_threshold(): async Text{
       let r = Nat.toText(freezingThreshold);
       return r;
    };
    public func ui_service_compute_allocation(): async Text{
       let r = Nat.toText(computeAllocation);
       return r;
    };
    public func ui_service_memory_allocation(): async Text{
       let r = Nat.toText(memoryAllocation);
       return r;
     };
    public func ui_service_generated_buckets(): async Text{
       var i = 0;
       for(b in buckets.vals()){
             switch(b){
                 case(null){};
                 case(?b){ i := i + 1; };
             };
       };
       let r = Nat.toText(i);
       return r;
    };
    public func ui_service_using_memory_size(): async Text{
       var i: Nat = 0;
       for(b in buckets.vals()){
             switch(b){
                 case(null){};
                 case(?b){
                   var sm: Nat = await b.get_rts_memory_size();
                   i := i + sm;
                 };
             };
       };
       let r = Nat.toText(i);
       return r;
    };
    public func ui_service_created_tables(): async Text{
       var i = 0;
       var tables = await get_tables();
       for(t in tables.vals()){
             i := i + 1;
       };
       let r = Nat.toText(i);
       return r;
    };
}