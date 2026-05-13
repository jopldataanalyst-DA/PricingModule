(function(){
  var themes = [
    ["forest","Forest"],["midnight","Midnight"],["slate","Slate"],["ivory","Ivory"],["arctic","Arctic"],
    ["emerald","Emerald"],["crimson","Crimson"],["amber","Amber"],["violet","Violet"],["rose","Rose"],
    ["teal","Teal"],["cobalt","Cobalt"],["graphite","Graphite"],["plum","Plum"],["moss","Moss"],
    ["sky","Sky"],["sand","Sand"],["mint","Mint"],["coffee","Coffee"],["mono","Mono"]
  ];
  var key = "erp-theme";
  function applyTheme(id){
    id = id || "forest";
    document.documentElement.setAttribute("data-theme", id);
    localStorage.setItem(key, id);
    var selects = document.querySelectorAll("[data-theme-select]");
    for(var i=0;i<selects.length;i++) selects[i].value = id;
  }
  function wireSelects(){
    var current = localStorage.getItem(key) || "forest";
    var selects = document.querySelectorAll("[data-theme-select]");
    for(var i=0;i<selects.length;i++){
      var select = selects[i];
      if(!select.options.length){
        for(var j=0;j<themes.length;j++){
          var option = document.createElement("option");
          option.value = themes[j][0];
          option.textContent = themes[j][1];
          select.appendChild(option);
        }
      }
      select.value = current;
      select.onchange = function(){ applyTheme(this.value); };
    }
    applyTheme(current);
  }
  applyTheme(localStorage.getItem(key) || "forest");
  if(document.readyState === "loading") document.addEventListener("DOMContentLoaded", wireSelects);
  else wireSelects();
})();
