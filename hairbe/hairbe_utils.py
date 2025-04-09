def get_extension_from_content_type(content_type: str) -> str:
    """content_type에서 적절한 파일 확장자를 결정하는 함수"""
    if not content_type:
        return '.jpg'  # 기본 확장자
        
    content_type = content_type.lower()
    
    # 일반적인 이미지 MIME 타입에 따른 확장자 매핑
    if 'jpeg' in content_type or 'jpg' in content_type:
        return '.jpg'
    elif 'png' in content_type:
        return '.png'
    elif 'webp' in content_type:
        return '.webp'
    elif 'gif' in content_type:
        return '.gif'
    elif 'bmp' in content_type:
        return '.bmp'
    elif 'tiff' in content_type:
        return '.tiff'
    elif 'svg' in content_type:
        return '.svg'
    
    # 알 수 없는 타입은 기본 확장자 사용
    return '.jpg'