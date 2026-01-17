def is_pattern(s: str) -> bool:
    return "+" in s or "#" in s

def match_pattern(pattern: str, topic: str) -> bool:
    p_levels = pattern.split("/")
    t_levels = topic.split("/")
    
    return _match_levels(p_levels, t_levels, 0, 0)

def _match_levels(p_levels, t_levels, p_idx, t_idx) -> bool:
    if p_idx == len(p_levels) and t_idx == len(t_levels):
        return True
    if p_idx == len(p_levels):
        return False
    
    if p_levels[p_idx] == "#":
        return True
    
    if t_idx == len(t_levels):
        return False
    
    if p_levels[p_idx] == "+" or p_levels[p_idx] == t_levels[t_idx]:
        return _match_levels(p_levels, t_levels, p_idx + 1, t_idx + 1)
    
    return False
